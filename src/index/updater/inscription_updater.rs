use super::*;

use std::fs::File;
use serde_json::Value;
use hex;

#[derive(Debug, PartialEq, Copy, Clone)]
enum Curse {
  DuplicateField,
  IncompleteField,
  NotAtOffsetZero,
  NotInFirstInput,
  Pointer,
  Pushnum,
  Reinscription,
  Stutter,
  UnrecognizedEvenField,
}

#[derive(Debug, Clone)]
pub(super) struct Flotsam<'a> {
  inscription_id: InscriptionId,
  offset: u64,
  origin: Origin,
  tx_option: Option<&'a Transaction>,
}

#[derive(Debug, Clone)]
enum Origin {
  New {
    cursed: bool,
    fee: u64,
    hidden: bool,
    parents: Vec<InscriptionId>,
    reinscription: bool,
    unbound: bool,
    vindicated: bool,
  },
  Old {
    sequence_number: u32,
    old_satpoint: SatPoint,
  },
}

pub(super) struct InscriptionUpdater<'a, 'tx> {
  pub(super) blessed_inscription_count: u64,
  pub(super) cursed_inscription_count: u64,
  pub(super) flotsam: Vec<Flotsam<'a>>,
  pub(super) height: u32,
  pub(super) home_inscription_count: u64,
  pub(super) home_inscriptions: &'a mut Table<'tx, u32, InscriptionIdValue>,
  pub(super) id_to_sequence_number: &'a mut Table<'tx, InscriptionIdValue, u32>,
  pub(super) inscription_number_to_sequence_number: &'a mut Table<'tx, i32, u32>,
  pub(super) lost_sats: u64,
  pub(super) next_sequence_number: u32,
  pub(super) reward: u64,
  pub(super) transaction_buffer: Vec<u8>,
  pub(super) transaction_id_to_transaction: &'a mut Table<'tx, &'static TxidValue, &'static [u8]>,
  pub(super) sat_to_sequence_number: &'a mut MultimapTable<'tx, u64, u32>,
  pub(super) sequence_number_to_children: &'a mut MultimapTable<'tx, u32, u32>,
  pub(super) sequence_number_to_entry: &'a mut Table<'tx, u32, InscriptionEntryValue>,
  pub(super) timestamp: u32,
  pub(super) unbound_inscriptions: u64,
  pub(super) first_in_block: bool,
}

impl<'a, 'tx> InscriptionUpdater<'a, 'tx> {
  pub(super) fn index_inscriptions(
    &mut self,
    tx: &'a Transaction,
    txid: Txid,
    input_utxo_entries: &[ParsedUtxoEntry],
    output_utxo_entries: &mut [UtxoEntryBuf],
    utxo_cache: &mut HashMap<OutPoint, UtxoEntryBuf>,
    index: &Index,
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
  ) -> Result {
    let mut floating_inscriptions = Vec::new();
    let mut id_counter = 0;
    let mut inscribed_offsets = BTreeMap::new();
    let jubilant = self.height >= index.settings.chain().jubilee_height();
    let mut total_input_value = 0;
    let total_output_value = tx.output.iter().map(|txout| txout.value).sum::<u64>();

    let envelopes = ParsedEnvelope::from_transaction(tx);
    let has_new_inscriptions = !envelopes.is_empty();
    let mut envelopes = envelopes.into_iter().peekable();

    for (input_index, txin) in tx.input.iter().enumerate() {
      // skip subsidy since no inscriptions possible
      if txin.previous_output.is_null() {
        total_input_value += Height(self.height).subsidy();
        continue;
      }

      let mut transferred_inscriptions = input_utxo_entries[input_index].parse_inscriptions();

      transferred_inscriptions.sort_by_key(|(sequence_number, _)| *sequence_number);

      for (sequence_number, old_satpoint_offset) in transferred_inscriptions {
        let old_satpoint = SatPoint {
          outpoint: txin.previous_output,
          offset: old_satpoint_offset,
        };

        let inscription_id = InscriptionEntry::load(
          self
            .sequence_number_to_entry
            .get(sequence_number)?
            .unwrap()
            .value(),
        )
        .id;

        let offset = total_input_value + old_satpoint_offset;
        floating_inscriptions.push(Flotsam {
          offset,
          inscription_id,
          origin: Origin::Old {
            sequence_number,
            old_satpoint,
          },
          tx_option: Some(&tx),
        });

        inscribed_offsets
          .entry(offset)
          .or_insert((inscription_id, 0))
          .1 += 1;
      }

      let offset = total_input_value;

      let input_value = input_utxo_entries[input_index].total_value();
      total_input_value += input_value;

      // go through all inscriptions in this input
      while let Some(inscription) = envelopes.peek() {
        if inscription.input != u32::try_from(input_index).unwrap() {
          break;
        }

        let inscription_id = InscriptionId {
          txid,
          index: id_counter,
        };

        let curse = if inscription.payload.unrecognized_even_field {
          Some(Curse::UnrecognizedEvenField)
        } else if inscription.payload.duplicate_field {
          Some(Curse::DuplicateField)
        } else if inscription.payload.incomplete_field {
          Some(Curse::IncompleteField)
        } else if inscription.input != 0 {
          Some(Curse::NotInFirstInput)
        } else if inscription.offset != 0 {
          Some(Curse::NotAtOffsetZero)
        } else if inscription.payload.pointer.is_some() {
          Some(Curse::Pointer)
        } else if inscription.pushnum {
          Some(Curse::Pushnum)
        } else if inscription.stutter {
          Some(Curse::Stutter)
        } else if let Some((id, count)) = inscribed_offsets.get(&offset) {
          if *count > 1 {
            Some(Curse::Reinscription)
          } else {
            let initial_inscription_sequence_number =
              self.id_to_sequence_number.get(id.store())?.unwrap().value();

            let entry = InscriptionEntry::load(
              self
                .sequence_number_to_entry
                .get(initial_inscription_sequence_number)?
                .unwrap()
                .value(),
            );

            let initial_inscription_was_cursed_or_vindicated =
              entry.inscription_number < 0 || Charm::Vindicated.is_set(entry.charms);

            if initial_inscription_was_cursed_or_vindicated {
              None
            } else {
              Some(Curse::Reinscription)
            }
          }
        } else {
          None
        };

        let offset = inscription
          .payload
          .pointer()
          .filter(|&pointer| pointer < total_output_value)
          .unwrap_or(offset);

        floating_inscriptions.push(Flotsam {
          inscription_id,
          offset,
          origin: Origin::New {
            cursed: curse.is_some() && !jubilant,
            fee: 0,
            hidden: inscription.payload.hidden(),
            parents: inscription.payload.parents(),
            reinscription: inscribed_offsets.contains_key(&offset),
            unbound: input_value == 0
              || curse == Some(Curse::UnrecognizedEvenField)
              || inscription.payload.unrecognized_even_field,
            vindicated: curse.is_some() && jubilant,
          },
          tx_option: Some(&tx),
        });

        inscribed_offsets
          .entry(offset)
          .or_insert((inscription_id, 0))
          .1 += 1;

        envelopes.next();
        id_counter += 1;
      }
    }

    if index.index_transactions && has_new_inscriptions {
      tx.consensus_encode(&mut self.transaction_buffer)
        .expect("in-memory writers don't error");

      self
        .transaction_id_to_transaction
        .insert(&txid.store(), self.transaction_buffer.as_slice())?;

      self.transaction_buffer.clear();
    }

    let potential_parents = floating_inscriptions
      .iter()
      .map(|flotsam| flotsam.inscription_id)
      .collect::<HashSet<InscriptionId>>();

    for flotsam in &mut floating_inscriptions {
      if let Flotsam {
        origin: Origin::New {
          parents: purported_parents,
          ..
        },
        ..
      } = flotsam
      {
        let mut seen = HashSet::new();
        purported_parents
          .retain(|parent| seen.insert(*parent) && potential_parents.contains(parent));
      }
    }

    // still have to normalize over inscription size
    for flotsam in &mut floating_inscriptions {
      if let Flotsam {
        origin: Origin::New { ref mut fee, .. },
        ..
      } = flotsam
      {
        *fee = (total_input_value - total_output_value) / u64::from(id_counter);
      }
    }

    let is_coinbase = tx
      .input
      .first()
      .map(|tx_in| tx_in.previous_output.is_null())
      .unwrap_or_default();

    let own_inscription_cnt = floating_inscriptions.len();
    if is_coinbase {
      floating_inscriptions.append(&mut self.flotsam);
    }

    floating_inscriptions.sort_by_key(|flotsam| flotsam.offset);
    let mut inscriptions = floating_inscriptions.into_iter().peekable();

    let mut new_locations = Vec::new();
    let mut output_value = 0;
    let mut inscription_idx = 0;
    for (vout, txout) in tx.output.iter().enumerate() {
      let end = output_value + txout.value;

      while let Some(flotsam) = inscriptions.peek() {
        if flotsam.offset >= end {
          break;
        }

        let sent_to_coinbase = inscription_idx >= own_inscription_cnt;
        inscription_idx += 1;

        let new_satpoint = SatPoint {
          outpoint: OutPoint {
            txid,
            vout: vout.try_into().unwrap(),
          },
          offset: flotsam.offset - output_value,
        };

        new_locations.push((
          new_satpoint,
          sent_to_coinbase,
          txout,
          inscriptions.next().unwrap(),
          txout.script_pubkey.is_op_return(),
        ));
      }

      output_value = end;
    }

    for (new_satpoint, sent_to_coinbase, txout, flotsam, op_return) in new_locations.into_iter() {
      let output_utxo_entry =
        &mut output_utxo_entries[usize::try_from(new_satpoint.outpoint.vout).unwrap()];

      let tx = flotsam.tx_option.clone().unwrap();
      self.update_inscription_location(
        Some(&tx),
        Some(&txout.script_pubkey),
        Some(&txout.value),
        sent_to_coinbase,

        input_sat_ranges,
        flotsam,
        new_satpoint,
        op_return,
        Some(output_utxo_entry),
        utxo_cache,
        index,
      )?;
    }

    if is_coinbase {
      for flotsam in inscriptions {
        let new_satpoint = SatPoint {
          outpoint: OutPoint::null(),
          offset: self.lost_sats + flotsam.offset - output_value,
        };

        let tx = flotsam.tx_option.clone().unwrap();
        self.update_inscription_location(
          Some(&tx), None, None, true,
          input_sat_ranges,
          flotsam,
          new_satpoint,
          false,
          None,
          utxo_cache,
          index,
        )?;
      }
      self.lost_sats += self.reward - output_value;
      Ok(())
    } else {

      for flotsam in inscriptions {
        self.flotsam.push(Flotsam {
          offset: self.reward + flotsam.offset - output_value,
          ..flotsam
        });

        // ord indexes sent as fee transfers at the end of the block but it would make more sense if they were indexed as soon as they are sent
        self.write_to_file(format!("cmd;{0};insert;early_transfer_sent_as_fee;{1};{2}", self.height, flotsam.inscription_id, txid), true)?;
      }
      self.reward += total_input_value - output_value;
      Ok(())
    }
  }

  fn calculate_sat(
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
    input_offset: u64,
  ) -> Option<Sat> {
    let input_sat_ranges = input_sat_ranges?;

    let mut offset = 0;
    for (start, end) in input_sat_ranges {
      let size = end - start;
      if offset + size > input_offset {
        let n = start + input_offset - offset;
        return Some(Sat(n));
      }
      offset += size;
    }

    unreachable!()
  }

  fn is_text(inscription_content_type_option: &Option<Vec<u8>>) -> bool {
    if inscription_content_type_option.is_none() { return false; }

    let inscription_content_type = inscription_content_type_option.as_ref().unwrap();
    let inscription_content_type_str = std::str::from_utf8(&inscription_content_type).unwrap_or("");
    return inscription_content_type_str == "text/plain" || inscription_content_type_str.starts_with("text/plain;") ||
      inscription_content_type_str == "application/json" || inscription_content_type_str.starts_with("application/json;"); // NOTE: added application/json for JSON5 etc.
  }

  fn is_json(inscription_content_option: &Option<Vec<u8>>) -> bool {
    if inscription_content_option.is_none() { return false; }
    let inscription_content = inscription_content_option.as_ref().unwrap();

    let json = serde_json::from_slice::<Value>(&inscription_content);
    if json.is_err() {
      return false;
    } else {
      return true
    }
  }

  fn is_brc20(inscription_content_option: &Option<Vec<u8>>) -> bool {
    if inscription_content_option.is_none() { return false; }
    let inscription_content = inscription_content_option.as_ref().unwrap();
    match serde_json::from_slice::<Value>(&inscription_content) {
      Ok(content) => {
        if let Value::Object(map) = content {
          // p
          if let Some(p) = map.get("p") {
            if p.as_str() == Some("brc-20") {
              // op
              if let Some(op) = map.get("op") {
                if op.as_str() != Some("deploy") && op.as_str() != Some("mint") && op.as_str() != Some("transfer") {
                  return false;
                }
              } else {
                return false;
              }

              // tick
              if map.contains_key("tick") {
                return true;
              } else{
                return false
              }
            }else if p.as_str() == Some("brc20-module") {
              return true;
            } else if p.as_str() == Some("brc20-swap") {
              return true;
            } else {
              return false;
            }
          } else {
            return false;
          }
        } else {
          false
        }
      },
      Err(_) => false,
    }
  }

  fn write_to_file(
    &mut self,
    to_write: String,
    flush: bool,
  ) -> Result {
    lazy_static! {
      static ref LOG_FILE: Mutex<Option<File>> = Mutex::new(None);
    }
    let mut log_file = LOG_FILE.lock().unwrap();
    if log_file.as_ref().is_none() {
      *log_file = Some(File::options().append(true).open(format!("log_file.txt")).unwrap());
    }
    if to_write != "" {
      if self.first_in_block {
        writeln!(log_file.as_ref().unwrap(), "cmd;{0};block_start;{1}", self.height, self.timestamp)?;
      }
      self.first_in_block = false;

      writeln!(log_file.as_ref().unwrap(), "{}", to_write)?;
    }
    if flush {
      (log_file.as_ref().unwrap()).flush()?;
    }

    Ok(())
  }

  pub(super) fn end_block(
    &mut self,
  ) -> Result {
    if !self.first_in_block {
      self.write_to_file(format!("cmd;{0};block_end", self.height), true)?;
    }

    Ok(())
  }

  fn update_inscription_location(
    &mut self,
    tx_option: Option<&Transaction>,
    new_script_pubkey: Option<&ScriptBuf>,
    new_output_value: Option<&u64>,
    send_to_coinbase: bool,
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
    flotsam: Flotsam,
    new_satpoint: SatPoint,
    op_return: bool,
    mut normal_output_utxo_entry: Option<&mut UtxoEntryBuf>,
    utxo_cache: &mut HashMap<OutPoint, UtxoEntryBuf>,
    index: &Index,
  ) -> Result {
    let tx = tx_option.unwrap();
    let inscription_id = flotsam.inscription_id;

    let (unbound, sequence_number) = match flotsam.origin {
      Origin::Old {
        sequence_number,
        old_satpoint,
      } => {
        let entry = InscriptionEntry::load(
          self
            .sequence_number_to_entry
            .get(&sequence_number)?
            .unwrap()
            .value(),
        );

        if op_return {
          let mut charms = entry.charms;
          Charm::Burned.set(&mut charms);

          self.sequence_number_to_entry.insert(
            sequence_number,
            &InscriptionEntry { charms, ..entry }.store(),
          )?;
        }

        if let Some(ref sender) = index.event_sender {
          sender.blocking_send(Event::InscriptionTransferred {
            block_height: self.height,
            inscription_id,
            new_location: new_satpoint,
            old_location: old_satpoint,
            sequence_number,
          })?;
        }

        // get is_json_or_text from id_to_entry
        let is_brc20 = entry.is_brc20;
        if is_brc20 { // only track non-cursed and first two transactions
          self.write_to_file(format!("cmd;{0};insert;transfer;{1};{old_satpoint};{new_satpoint};{send_to_coinbase};{2};{3}",
                    self.height, flotsam.inscription_id,
                    hex::encode(new_script_pubkey.unwrap_or(&ScriptBuf::new()).clone().into_bytes()),
                    new_output_value.unwrap_or(&0)), false)?;
        }


        (false, sequence_number)
      }
      Origin::New {
        cursed,
        fee,
        hidden,
        parents,
        reinscription,
        unbound,
        vindicated,
      } => {
        let inscription_number = if cursed {
          let number: i32 = self.cursed_inscription_count.try_into().unwrap();
          self.cursed_inscription_count += 1;
          -(number + 1)
        } else {
          let number: i32 = self.blessed_inscription_count.try_into().unwrap();
          self.blessed_inscription_count += 1;
          number
        };

        let sequence_number = self.next_sequence_number;
        self.next_sequence_number += 1;

        self
          .inscription_number_to_sequence_number
          .insert(inscription_number, sequence_number)?;

        let inscription = ParsedEnvelope::from_transaction(&tx)
            .get(flotsam.inscription_id.index as usize)
            .unwrap()
            .payload.clone();
        let inscription_content = inscription.body;
        let inscription_content_type = inscription.content_type;
        let inscription_metaprotocol = inscription.metaprotocol;

        let mut is_brc20 = false;
        let is_json = Self::is_json(&inscription_content);
        let is_text = Self::is_text(&inscription_content_type);
        if is_text && is_json && !vindicated {
          if Self::is_brc20(&inscription_content) {
            is_brc20 = true;
          }
        }

        if !unbound && is_brc20 {
          self.write_to_file(format!("cmd;{0};insert;number_to_id;{1};{2}", self.height, inscription_number, flotsam.inscription_id), false)?;
          let inscription_content_hex_str = hex::encode(inscription_content.unwrap_or(Vec::new()));
          let inscription_content_type_str = hex::encode(inscription_content_type.unwrap_or(Vec::new()));
          let inscription_metaprotocol_str = hex::encode(inscription_metaprotocol.unwrap_or(Vec::new()));
          self.write_to_file(format!("cmd;{0};insert;content;{1};{2};{3};{4};{5}",
                                     self.height, flotsam.inscription_id, is_json, inscription_content_type_str, inscription_metaprotocol_str, inscription_content_hex_str), false)?;

        }

        let sat = if unbound {
          None
        } else {
          Self::calculate_sat(input_sat_ranges, flotsam.offset)
        };

        let mut charms = 0;

        if cursed {
          Charm::Cursed.set(&mut charms);
        }

        if reinscription {
          Charm::Reinscription.set(&mut charms);
        }

        if let Some(sat) = sat {
          charms |= sat.charms();
        }

        if op_return {
          Charm::Burned.set(&mut charms);
        }

        if new_satpoint.outpoint == OutPoint::null() {
          Charm::Lost.set(&mut charms);
        }

        if unbound {
          Charm::Unbound.set(&mut charms);
        }

        if vindicated {
          Charm::Vindicated.set(&mut charms);
        }

        if let Some(Sat(n)) = sat {
          self.sat_to_sequence_number.insert(&n, &sequence_number)?;
        }

        let parent_sequence_numbers = parents
          .iter()
          .map(|parent| {
            let parent_sequence_number = self
              .id_to_sequence_number
              .get(&parent.store())?
              .unwrap()
              .value();

            self
              .sequence_number_to_children
              .insert(parent_sequence_number, sequence_number)?;

            Ok(parent_sequence_number)
          })
          .collect::<Result<Vec<u32>>>()?;

        if let Some(ref sender) = index.event_sender {
          sender.blocking_send(Event::InscriptionCreated {
            block_height: self.height,
            charms,
            inscription_id,
            location: (!unbound).then_some(new_satpoint),
            parent_inscription_ids: parents,
            sequence_number,
          })?;
        }

        self.sequence_number_to_entry.insert(
          sequence_number,
          &InscriptionEntry {
            charms,
            fee,
            height: self.height,
            id: inscription_id,
            inscription_number,
            parents: parent_sequence_numbers,
            sat,
            sequence_number,
            timestamp: self.timestamp,
            is_brc20,
          }
          .store(),
        )?;

        self
          .id_to_sequence_number
          .insert(&inscription_id.store(), sequence_number)?;

        if !hidden {
          self
            .home_inscriptions
            .insert(&sequence_number, inscription_id.store())?;

          if self.home_inscription_count == 100 {
            self.home_inscriptions.pop_first()?;
          } else {
            self.home_inscription_count += 1;
          }
        }

        if !unbound && is_brc20 {
          self.write_to_file(format!("cmd;{0};insert;transfer;{1};;{new_satpoint};{send_to_coinbase};{2};{3};1",
                                     self.height, flotsam.inscription_id,
                                     hex::encode(new_script_pubkey.unwrap_or(&ScriptBuf::new()).clone().into_bytes()),
                                     new_output_value.unwrap_or(&0)), false)?;
        }

        (unbound, sequence_number)
      }
    };

    let satpoint = if unbound {
      let new_unbound_satpoint = SatPoint {
        outpoint: unbound_outpoint(),
        offset: self.unbound_inscriptions,
      };
      self.unbound_inscriptions += 1;
      normal_output_utxo_entry = None;
      new_unbound_satpoint
    } else {
      new_satpoint
    };

    // The special outpoints, i.e., the null outpoint and the unbound outpoint,
    // don't follow the normal rulesr. Unlike real outputs they get written to
    // more than once. So we create a new UTXO entry here and commit() will
    // merge it with any existing entry.
    let output_utxo_entry = normal_output_utxo_entry.unwrap_or_else(|| {
      assert!(Index::is_special_outpoint(satpoint.outpoint));
      utxo_cache
        .entry(satpoint.outpoint)
        .or_insert(UtxoEntryBuf::empty(index))
    });

    output_utxo_entry.push_inscription(sequence_number, satpoint.offset, index);

    self.write_to_file("".to_string(), true)?;

    Ok(())
  }
}
