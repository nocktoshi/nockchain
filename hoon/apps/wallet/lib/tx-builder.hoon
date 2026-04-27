/=  transact  /common/tx-engine
/=  wutils  /apps/wallet/lib/utils
/=  wt  /apps/wallet/lib/types
/=  zo  /common/zoon
/=  bridge  /apps/bridge/types
::
::  Builds m-to-n transaction that can emit both simple PKH and multisig locks.
|_  bc=blockchain-constants:transact
+*  t  ~(. transact bc)
    utils  ~(. wutils %.y bc)
++  build
|=  $:  names=(list nname:transact)
        orders=(list order:wt)
        fee=coins:transact
        allow-low-fee=?
        sign-keys=(list schnorr-seckey:transact)
        refund-pkh=(unit hash:transact)
        get-note=$-(nname:transact nnote:transact)
        include-data=?
        note-selection=selection-strategy:wt
        height=page-number:transact
    ==
|^
^-  $:  spends:v1:transact
        witness-data:wt
        display=transaction-display:wt
    ==
=+  orders-valid=(orders-valid orders)
?:  ?=(%.n -.orders-valid)
  ~|("One or more orders are invalid. Reason: {<p.orders-valid>}" !!)
=/  signer-pubkeys=(list schnorr-pubkey:transact)
  %+  turn  sign-keys
  |=  sk=schnorr-seckey:transact
  %-  from-sk:schnorr-pubkey:transact
  (to-atom:schnorr-seckey:transact sk)
?~  signer-pubkeys
  ~|("At least one signing key is required" !!)
=/  sender-pubkey=schnorr-pubkey:transact  i.signer-pubkeys
=/  sender-pkh=hash:transact  (hash:schnorr-pubkey:transact sender-pubkey)
=/  notes=(list nnote:transact)  (turn names get-note)
=/  ascending=?  ?=(%asc note-selection)
::  If all notes are v0
=/  [raw-spends=spends:v1:transact =witness-data:wt display=transaction-display:wt]
  ?:  (levy notes |=(=nnote:transact ?=(^ -.nnote)))
    ?~  refund-pkh
      ~|('Need to specify a refund address if spending from v0 notes. Use the `--refund-pkh` flag in the create-tx command' !!)
    =/  notes-v0=(list nnote:v0:transact)
      %+  turn  notes
      |=  =nnote:transact
      ?>  ?=(^ -.nnote)
      nnote
    =.  notes-v0
      %+  sort  notes-v0
      |=  [a=nnote:v0:transact b=nnote:v0:transact]
      ?:(ascending (lth assets.a assets.b) (gth assets.a assets.b))
    =/  refund-lock=lock:transact  [%pkh [m=1 (z-silt:zo ~[u.refund-pkh])]]~
    (create-spends-0 notes-v0 orders fee sender-pubkey refund-lock)
  ::  If all notes are v1
  ?:  (levy notes |=(=nnote:transact ?=(@ -.nnote)))
    =/  notes-v1=(list nnote-1:v1:transact)
      %+  turn  notes
      |=  =nnote:transact
      ?>  ?=(@ -.nnote)
      nnote
    =.  notes-v1
      %+  sort  notes-v1
      |=  [a=nnote-1:v1:transact b=nnote-1:v1:transact]
      ?:(ascending (lth assets.a assets.b) (gth assets.a assets.b))
    =/  multisig-lock=(unit lock:transact)
      ::
      ::  ensure that all multisig locks are the same in the input notes
      |-
      ?~  notes-v1  ~
      ?^  lok=(multisig-lock i.notes-v1)
        =+  ref-fn=(first:nname:v1:transact (hash:lock:transact u.lok))
        ?:  %+  levy  `(list nnote-1:v1:transact)`notes-v1
            |=  note=nnote-1:v1:transact
            =(ref-fn ~(first-name get:nnote:transact note))
          lok
        ~|('Multisig detected in input. When a multisig is present, all inputs must share the same lock.' !!)
      $(notes-v1 t.notes-v1)
    =/  refund-lock=lock:transact
      ?^  refund-pkh
        [%pkh [m=1 (z-silt:zo ~[u.refund-pkh])]]~
      %+  fall  multisig-lock
      [%pkh [m=1 (z-silt:zo ~[sender-pkh])]]~
    (create-spends-1 notes-v1 orders fee sender-pkh refund-lock)
::
~|('Notes must all be the same version (mixed v0 / v1 note set).' !!)

=+  min-fee=(spends:estimate-fee:utils raw-spends inputs.display height)
?:  (lth fee min-fee)
  ?:  =(allow-low-fee %.n)
    ~|("Min fee not met. This transaction requires at least: {(trip (format-ui:common:display:utils min-fee))} nicks" !!)
    [raw-spends witness-data display]
  [raw-spends witness-data display]
::
::  helpers for building display metadata
::
++  update-display-0
  |=  $:  note=nnote:v0:transact
          display=transaction-display:wt
          addition=output-lock-map:wt
      ==
  ^-  transaction-display:wt
  ?>  ?=(%0 -.inputs.display)
  %=    display
      outputs
    (~(uni z-by:zo outputs.display) addition)
  ::
      inputs
    :-  %0
    %-  ~(put z-by:zo p.inputs.display)
    [name.note sig.note]
  ==
::
++  update-display-1
  |=  $:  name=nname:transact
          display=transaction-display:wt
          addition=output-lock-map:wt
          =lock:transact
      ==
  ^-  transaction-display:wt
  ?>  ?=(%1 -.inputs.display)
  %=    display
      outputs
    (~(uni z-by:zo outputs.display) addition)
  ::
      inputs
    :-  %1
    %-  ~(put z-by:zo p.inputs.display)
    ::  assert that the lock is a spend-condition
    ?>  ?=(^ -.lock)
    [name lock]
  ==
::
++  create-spends-0
  |=  $:  notes=(list nnote:v0:transact)
          orders=(list order:wt)
          fee=@
          pubkey=schnorr-pubkey:transact
          refund-lock=lock:transact
      ==
  ^-  [=spends:v1:transact witness-data:wt transaction-display:wt]
  =/  initial-state=spend-build-state:wt
    %*  .  *spend-build-state:wt
      fee      fee
      orders   orders
      wd       [%0 ~]
      display  [[%0 ~] ~]
    ==
  =/  final-state
    (process-spends-0 notes initial-state pubkey refund-lock)
  =+  remaining-orders=orders.final-state
  =+  remaining-fee=fee.final-state
  ?.  ?&  =(~ remaining-orders)
          =(0 remaining-fee)
      ==
    ~|('Insufficient funds to pay fee and gift' !!)
  [spends.final-state wd.final-state display.final-state]
::
++  process-spends-0
  |=  $:  notes=(list nnote:v0:transact)
          state=spend-build-state:wt
          pubkey=schnorr-pubkey:transact
          refund-lock=lock:transact
      ==
  ^-  spend-build-state:wt
  ?~  notes
    state
  =/  note  i.notes
  ?.  ?|  =(pubkeys.sig.note (z-silt:zo ~[pubkey]))
          ?&  =(1 m.sig.note)
              (~(has z-in:zo pubkeys.sig.note) pubkey)
          ==
      ==
    ~|('Note not spendable by signing key' !!)
  =/  [pending-orders=(list order:wt) specs=(list order:wt) remainder=@]
    (allocate-orders orders.state assets.note)
  =/  fee-portion=@  (min fee.state remainder)
  =/  new-fee=@  (sub fee.state fee-portion)
  =/  refund=@  (sub remainder fee-portion)
  =?  specs  !=(refund 0)
    [(build-refund-order refund refund-lock) specs]
  ?:  =(~ specs)
    %=  $
      notes   t.notes
    ==
  =/  [=seeds:v1:transact output-map=output-lock-map:wt]
    (seeds-from-specs specs note fee-portion)
  ?~  seeds
    ~|('No seeds were provided' !!)
  =/  spend=spend-0:v1:transact
    %*  .  *spend-0:v1:transact
      seeds  seeds
      fee    fee-portion
    ==
  %=  $
    notes          t.notes
    spends.state   (~(put z-by:zo spends.state) [name.note [%0 spend]])
    fee.state      new-fee
    orders.state   pending-orders
    display.state  (update-display-0 note display.state output-map)
    wd.state       (sign-spend name.note [%0 spend] wd.state)
  ==
::
++  create-spends-1
  |=  $:  notes=(list nnote-1:v1:transact)
          orders=(list order:wt)
          fee=@
          sender-pkh=hash:transact
          refund-lock=lock:transact
      ==
  ^-  [=spends:v1:transact witness-data:wt transaction-display:wt]
  =/  initial-state=spend-build-state:wt
    %*  .  *spend-build-state:wt
      fee      fee
      orders   orders
      wd       [%1 ~]
      display  [[%1 ~] ~]
    ==
  =/  final-state
    (process-spends-1 notes initial-state sender-pkh refund-lock)
  =+  remaining-orders=orders.final-state
  =+  remaining-fee=fee.final-state
  ?.  ?&  =(~ remaining-orders)
          =(0 remaining-fee)
      ==
    ~|('Insufficient funds to pay fee and gift' !!)
  =/  final-wd=witness-data:wt
    %+  roll  ~(tap z-by:zo spends.final-state)
    |=  [[nam=nname:transact sp=spend:v1:transact] acc=witness-data:wt]
    (sign-spend nam sp acc)
  [spends.final-state final-wd display.final-state]
::
++  process-spends-1
  |=  $:  notes=(list nnote-1:v1:transact)
          state=spend-build-state:wt
          sender-pkh=hash:transact
          refund-lock=lock:transact
      ==
  ^-  spend-build-state:wt
  ?~  notes
    state
  =/  note  i.notes
  =/  nd=(unit note-data:v1:transact)
    ((soft note-data:v1:transact) note-data.note)
  ?~  nd
    ~|('note-data malformed in note' !!)
  =+  pulled=(pull:locks:utils [u.nd name.note (some sender-pkh)])
  ?~  pulled
    =+  name-cord=(name:v1:display:utils name.note)
    ~|  "Error processing note {<name-cord>}. Reason: first-name did not correspond to a supported lock."  !!
  =/  pkh=(unit pkh:v1:transact)
    =/  input-lock=spend-condition:transact  u.pulled
    |-
    ?~  input-lock
      ~
    ?:  ?=(%pkh -.i.input-lock)
      `+.i.input-lock
    $(input-lock t.input-lock)
  =/  signable=?
    ?~  pkh  %.y
    %+  levy  sign-keys
    |=  sk=schnorr-seckey:transact
    %-  ~(has z-in:zo h.u.pkh)
    %-  hash:schnorr-pubkey:transact
    %-  from-sk:schnorr-pubkey:transact
    (to-atom:schnorr-seckey:transact sk)
  ?.  signable
    ~|  ^-  @t
        ;:  (cury cat 3)
            'One or more of the provided signing keys is not required by note '
            (name:v1:display:utils name.note)
            '.'
        ==
    !!
  =/  input-lock=lock:transact  u.pulled
  =/  allocation  (allocate-orders orders.state assets.note)
  =/  [pending-orders=(list order:wt) specs=(list order:wt) remainder=@]
    allocation
  =/  fee-portion=@  (min fee.state remainder)
  =/  new-fee=@  (sub fee.state fee-portion)
  =/  refund=@  (sub remainder fee-portion)
  =/  specs-with-refund=(list order:wt)
    ?:  =(refund 0)
      specs
    [(build-refund-order refund refund-lock) specs]
  ?:  =(~ specs-with-refund)
    $(notes t.notes)
  =/  [=seeds:v1:transact output-map=output-lock-map:wt]
    (seeds-from-specs specs-with-refund note fee-portion)
  ?~  seeds
    ~|('No seeds were provided' !!)
  =/  bythos-active=?
    (gte origin-page.note bythos-phase.bc)
  =/  lmp=lock-merkle-proof:transact
    ?:  bythos-active
      (build-lock-merkle-proof-full:lock:transact input-lock 1)
    (build-lock-merkle-proof-stub:lock:transact input-lock 1)
  =/  spend=spend-1:v1:transact
    %*  .  *spend-1:v1:transact
      seeds  seeds
      fee    fee-portion
    ==
  =.  witness.spend
    %*  .  *witness:transact
      lmp  lmp
    ==
  %=  $
    notes          t.notes
    spends.state   (~(put z-by:zo spends.state) [name.note [%1 spend]])
    fee.state      new-fee
    orders.state   pending-orders
    display.state  (update-display-1 name.note display.state output-map input-lock)
  ==
++  sign-spend
  |=  [name=nname:transact =spend:v1:transact wd=witness-data:wt]
  ^-  witness-data:wt
  ?-    -.spend
      %0
    ?>  ?=(%0 -.wd)
    :-  %0
    %+  ~(put z-by:zo p.wd)
      name
    =+  sig-hash=(sig-hash:spend:v1:transact spend)
    %+  roll  sign-keys
    |=  $:  sk=schnorr-seckey:transact
            acc=_signature.spend
        ==
    (sign:signature:transact acc sk sig-hash)
  ::
      %1
    ?>  ?=(%1 -.wd)
    :-  %1
    %+  ~(put z-by:zo p.wd)
      name
    =+  sig-hash=(sig-hash:spend:v1:transact spend)
    %+  roll  sign-keys
    |=  $:  sk=schnorr-seckey:transact
            acc=_witness.spend
        ==
    (sign:witness:transact acc sk sig-hash)
  ==
::
++  allocate-orders
  |=  [orders=(list order:wt) assets=@]
  ^-  [orders=(list order:wt) specs=(list order:wt) remainder=@]
  %+  roll  orders
  |=  $:  ord=order:wt
          next-orders=(list order:wt)
          out-orders=(list order:wt)
          rem=_assets
      ==
  ?:  =(0 rem)
    [[ord next-orders] out-orders rem]
  =/  gift-out  (order-gift ord)
  =/  take=@  (min gift-out rem)
  =.  rem  (sub rem take)
  =.  out-orders  [(with-gift ord take) out-orders]
  =?  next-orders  (lth take gift-out)
    [(with-gift ord (sub gift-out take)) next-orders]
  [next-orders out-orders rem]
::
++  seeds-from-specs
  |=  $:  specs=(list order:wt)
          note=nnote:transact
          fee-portion=@
      ==
  ^-  [seeds:v1:transact output-lock-map:wt]
  =;  [seeds=(list seed:v1:transact) total-gifts=@ =output-lock-map:wt]
    ?.  =(assets.note (add total-gifts fee-portion))
      ~|  "assets in must equal gift + fee + refund"  !!
    [(z-silt:zo seeds) output-lock-map]
  %+  roll  specs
  |=  $:  spec=order:wt
          seeds=(list seed:v1:transact)
          gifts=@
          =output-lock-map:wt
      ==
  =/  metadata=lock-metadata-1:wt  (extract-metadata spec)
  =?  include-data  ?=(%multisig -.spec)
    %.y
  =/  [lock-root=hash:transact base-entries=(list [key=@tas jam=@ val=*])]
    ?-    -.+.metadata
        %lock
      :-  (hash:lock:transact lock.metadata)
      ?:  include-data
        :~  [%lock 0 ^-(lock-data:wt [%0 lock.metadata])]
        ==
      ~
    ::
        %lock-root
      [root.metadata ~]
    ::
        %bridge-deposit
      :-  root.metadata
      :~  [%bridge 0 [%0 %base (evm-address-to-based:bridge addr.metadata)]]
      ==
    ==
  ::  optional opaque UTF-8 blob; wallet jams cord as atom under blob (consumer interprets text)
  =/  maybe-blob-text=(unit @t)
    ?-  -.spec
      %pkh              blob-data.spec
      %multisig         blob-data.spec
      %lock-root        blob-data.spec
      %bridge-deposit   blob-data.spec
    ==
  =/  blob-entries=(list [key=@tas jam=@ val=*])
    ?~  maybe-blob-text  ~
    ?:  =(0 (met 3 u.maybe-blob-text))
      ::
      ::  treat empty cord like ~
      ~
    =/  jam=@  (jam u.maybe-blob-text)
    =/  maybe=(unit *)  ((soft *) (cue jam))
    ?~  maybe
      ~|('wallet: blob jam did not cue to a valid noun' !!)
    ~[[%blob jam u.maybe]]
  =/  maybe-memo=(unit memo-data:wt)
    ?-  -.spec
      %pkh              memo.spec
      %multisig         memo.spec
      %lock-root        memo.spec
      %bridge-deposit   memo.spec
    ==
  =/  memo-entries=(list [key=@tas jam=@ val=*])
    ?~  maybe-memo  ~
    :~  [%memo 0 u.maybe-memo]
    ==
  =/  all-entries=(list [key=@tas jam=@ val=*])
    ;:  weld
      base-entries
      blob-entries
      memo-entries
    ==
  ::  One insert per %tas key (last wins): base then blobs then memo — same
  ::  layering as planner output note-data rows before z-map encoding.
  =/  deduped-pairs=(list [key=@tas val=*])
    %+  roll  all-entries
    |=  [[key=@tas jam=@ val=*] acc=(list [key=@tas val=*])]
    =/  without-old=(list [key=@tas val=*])
      %+  skim  acc
      |=  [k=@tas v=*]
      !=(k key)
    (snoc without-old [key val])
  =/  nd=note-data:v1:transact
    %+  roll  deduped-pairs
    |=  [[key=@tas val=*] acc=note-data:v1:transact]
    (~(put z-by:zo acc) key val)
  =/  seed=seed:v1:transact
    :*  output-source=~
        lock-root=lock-root
        note-data=nd
        gift=(order-gift spec)
        parent-hash=(hash:nnote:transact note)
    ==
  :*  [seed seeds]
      (add gifts (order-gift spec))
      (~(put z-by:zo output-lock-map) (first:nname:transact lock-root.seed) metadata)
  ==
::
++  memo-order-valid
  |=  ord=order:wt
  ^-  ?
  ::  memo is optional: ~ means omit; only validate when a payload is present
  =/  maybe-memo=(unit memo-data:wt)
    ?-  -.ord
      %pkh              memo.ord
      %multisig         memo.ord
      %lock-root        memo.ord
      %bridge-deposit   memo.ord
    ==
  ?~  maybe-memo  %.y
  =/  m=memo-data:wt  u.maybe-memo
  ?&  (gth (lent m) 0)
      (lte (lent m) 2.048)
  ==
::
++  blob-order-valid
  |=  ord=order:wt
  ^-  ?
  ::  blob is optional; if present, cord must be non-empty (same idea as memo)
  =/  maybe-blob=(unit @t)
    ?-  -.ord
      %pkh              blob-data.ord
      %multisig         blob-data.ord
      %lock-root        blob-data.ord
      %bridge-deposit   blob-data.ord
    ==
  ?~  maybe-blob  %.y
  (gth (met 3 u.maybe-blob) 0)
::
++  orders-valid
  |=  orders=(list order:wt)
  ^-  (reason:transact ~)
  ?:  =(0 (lent orders))
    [%.n 'cannot create transaction with no orders']
  =|  num-bridge-orders=@
  |-
  ?~  orders
    ?:  (gth num-bridge-orders 1)
      [%.n %bridge-orders-exceeds-one]
    [%.y ~]
  =/  ord=order:wt  i.orders
  ?.  (memo-order-valid ord)
    [%.n %memo-invalid]
  ?.  (blob-order-valid ord)
    [%.n %blob-invalid]
  ?-    -.ord
      %pkh
    ?:  =(0 gift.ord)
      [%.n %gift-cannot-be-zero]
    $(orders t.orders)
  ::
      %multisig
    =/  participants=(list hash:transact)  participants.ord
    =/  unique=@ud
      ~(wyt z-in:zo (z-silt:zo participants))
    ?:  =(participants ~)
      [%.n 'Multisig order must include at least one participant']
    ?:  (lte threshold.ord 0)
      [%.n 'Multisig threshold must be greater than zero']
    ?:  (gth threshold.ord (lent participants))
      [%.n 'Multisig threshold cannot exceed number of participants']
    ?:  (lth unique (lent participants))
      [%.n 'Multisig participants must be unique']
    ?:  =(0 gift.ord)
      [%.n 'order must include a gift greater than 0']
    $(orders t.orders)
  ::
     %bridge-deposit
   ?.  (gte gift.ord (mul *minimum-event-nocks:bridge nicks-per-nock:transact))
     [%.n %gift-amount-is-less-than-minimum-bridge-deposit]
   $(orders t.orders, num-bridge-orders +(num-bridge-orders))
  ::
     %lock-root
    ?:  =(0 gift.ord)
      [%.n %gift-cannot-be-zero]
    $(orders t.orders)
  ==
::
++  order-gift
  |=  ord=order:wt
  ^-  coins:transact
  ?-    -.ord
      %pkh       gift.ord
      %multisig  gift.ord
      %lock-root  gift.ord
      %bridge-deposit  gift.ord
    ==
::
++  with-gift
  |=  [ord=order:wt gift=coins:transact]
  ^-  order:wt
  ?-    -.ord
      %pkh       ord(gift gift)
      %multisig  ord(gift gift)
      %lock-root  ord(gift gift)
      %bridge-deposit  ord(gift gift)
    ==
::
++  extract-metadata
  |=  ord=order:wt
  ^-  lock-metadata-1:wt
  :-  %1
  ?-    -.ord
      %bridge-deposit
    [%bridge-deposit root=bridge-lock-root-default:bridge addr=address.ord]
  ::
      %lock-root
    [%lock-root root=root.ord]
  ::
      %pkh
    [%lock [%pkh [m=1 (z-silt:zo ~[recipient.ord])]]~ include-data]
    ::
      %multisig
    =/  participants=(list hash:transact)  participants.ord
    =/  allowed=(z-set:zo hash:transact)  (z-silt:zo participants)
    [%lock [%pkh [m=threshold.ord allowed]]~ include-data=%.y]
  ==
::
++  order-from-lock
  |=  [lok=lock:transact gift=@]
  ^-  (unit order:wt)
  ?@  -.lok  ~
  =/  primitive=lock-primitive:transact  i.lok
  ?.  ?=(%pkh -.primitive)
    ~
  =/  threshold=@  m.primitive
  =/  allowed=(z-set:zo hash:transact)  h.primitive
  =/  participants=(list hash:transact)  ~(tap z-in:zo allowed)
  ?~  participants
    ~|('Invalid lock, no participants specified.' !!)
    ?:  &(=(threshold 1) =(1 (lent participants)))
    (some [%pkh recipient=i.participants gift=gift memo=~ blob=~])
  (some [%multisig threshold=threshold participants=participants gift=gift memo=~ blob=~])
::
++  build-refund-order
  |=  [refund=@ refund-lock=lock:transact]
  ^-  order:wt
  ?~  parsed=(order-from-lock [refund-lock refund])
    ~|('Unsupported owner lock for refund; please specify --refund-pkh' !!)
  u.parsed
::
++  multisig-lock
 |=  note=nnote-1:v1:transact
 ^-  (unit lock:transact)
 ?~  lock-noun=(~(get z-by:zo note-data.note) %lock)
   ~
 ?~  soft-lock=((soft lock-data:wt) u.lock-noun)
   ~
 =+  pulled=lock.u.soft-lock
 ?@  -.pulled
   ~
 ?:  !=(1 (lent pulled))
   ~
 =/  lp=lock-primitive:transact  -.pulled
 ?.  ?=(%pkh -.lp)
   ~
 ?:  =(1 ~(wyt z-in:zo h.lp))
   ~
  `pulled
::
--
--
