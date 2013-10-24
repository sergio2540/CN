disable 'PhoneTimeOff'
drop 'PhoneTimeOff' 
#create 'PhoneTimeOff', {NAME=>'phoneId'}, {NAME=>'date'}, {NAME=>'minutesOff'}
#create 'PhoneTimeOff', {NAME=>'phoneId+date'}, {NAME=>'minutesOff'}
create 'PhoneTimeOff','NW', {NAME=>'secondsOff'}
