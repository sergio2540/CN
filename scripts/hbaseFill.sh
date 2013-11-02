disable 'Cell'
drop 'Cell'
disable 'phonePresence'
drop 'phonePresence'
create 'Cell', 'VC', {NAME=>'cellSequence'}, 'MO', {NAME=>'minutesOff'}
create 'phonePresence', 'PP', {NAME=>'phoneList'}
