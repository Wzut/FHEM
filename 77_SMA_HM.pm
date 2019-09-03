

package main;

use strict;
use warnings;
use bignum;
use IO::Socket::Multicast;
use Data::Dumper;

sub SMA_HM_Initialize($) 
{
  my ($hash) = @_;
  
  $hash->{ReadFn}        = "SMA_HM_Read";
  $hash->{DefFn}         = "SMA_HM_Define";
  $hash->{UndefFn}       = "SMA_HM_Undef";
  $hash->{AttrFn}        = "SMA_HM_Attr";
  $hash->{AttrList}      = "interval disable:1,0 detail-level:1,2 inverter1 inverter2 ".$readingFnAttributes;
}

sub SMA_HM_Define($$) 
{
  my ($hash, $def) = @_;
  my $name = $hash->{NAME};
  my @a = split("[ \t][ \t]*", $def);
  return "Usage: define $name SMA_HM <SHM serial number>" if(int(@a) != 3);

  $hash->{SERIAL}     = (defined($a[2])) ? $a[2] : "00000000";
  $hash->{LASTUPDATE} = time();
 
  my $socket = IO::Socket::Multicast->new(
           Proto     => 'udp',
           LocalPort => '9522',
           ReuseAddr => '1',
           ReusePort => defined(&ReusePort) ? 1 : 0); 
   
  if ($socket)
  {
   Log3 $hash, 3, "$name, multicast socket opened";
  
   $socket->mcast_add('239.12.255.254');

   $hash->{FD}    = $socket->fileno();
   $hash->{CD}    = $socket;         # sysread / close won't work on fileno
   $selectlist{$name} = $hash;  
   return undef;
  }

 my $txt = "failed to open multicast socket";
 Log3 $name, 3, "$name: $txt";
 return $txt;
}

sub SMA_HM_Undef($$) 
{
  my ($hash, $arg) = @_;
  my $name         = $hash->{NAME};
  my $socket       = $hash->{CD};
  
  $socket->mcast_drop('239.12.255.254');
  
  close($hash->{CD}) if($hash->{CD});
  delete($hash->{FD});
  delete($hash->{CD});
  delete($selectlist{$name});

  return;
}

sub SMA_HM_Attr 
{
  my ($cmd,$name,$aName,$aVal) = @_;
  my $hash = $defs{$name};
  my $do;
  
  # $cmd can be "del" or "set"
  # $name is device name
  # aName and aVal are Attribute name and value
  
  if ($aName eq "disable") 
  {
      if($cmd eq "set") {$do = ($aVal) ? 1 : 0; }
      $do = 0 if($cmd eq "del");
      my $val   = ($do == 1 ?  "disabled" : "initialized");
  
      readingsSingleUpdate($hash, "state", $val, 1);
  }
  
return undef;
}


# called from the global loop, when the select for hash->{FD} reports data
sub SMA_HM_Read($) 
{
  my ($hash) = @_;
  my $name   = $hash->{NAME};
  my $socket = $hash->{CD};
  my $data;
  
  return unless $socket->recv($data, 600); # Each SMA_HM packet is 600 bytes of packed payload
  return if (time() < ($hash->{LASTUPDATE}+int(AttrVal($name,"interval",60))));
  return if (!$init_done);
  return if (IsDisabled($name));
    
  # decode serial number of dataset received
  # unpack big-endian to 2-digit hex (bin2hex)
  my $hex       = unpack('H*', $data);
  my $serial    = hex(substr($hex,40,8));
   
  return if ($serial ne $hash->{SERIAL});
  

   # update time
  
  SMA_HM_DoParse($hash,$hex);
  $hash->{LASTUPDATE} = time();
  
 return undef;
}


sub SMA_HM_DoParse($$) 
{
 my ($hash,$hex) = @_;
 my $name = $hash->{NAME};
 
    # Format of the udp packets of the SMA_HM:
    # http://www.sma.de/fileadmin/content/global/Partner/Documents/SMA_Labs/EMETER-Protokoll-TI-de-10.pdf
    # http://www.eb-systeme.de/?page_id=1240

    # Conversion like in this python code:
    # http://www.unifox.at/sma_energy_meter/
    # https://github.com/datenschuft/SMA-EM

    ################ Aufbau Ergebnis-Array ####################
    # Extract datasets from hex:
    # Generic:
    # my $susyid       = hex(substr($hex,36,4));
    # $smaserial       = hex(substr($hex,40,8));
    # my $milliseconds = hex(substr($hex,48,8));

    # Counter Divisor: [Hex-Value]=Ws => Ws/1000*3600=kWh => divide by 3600000

   my (undef,$now_min, $now_hour ,undef,$now_month, $now_year, $now_day)  = localtime(time());  # jetzt
   my (undef,$last_min,$last_hour,undef,$last_month,$last_year,$last_day) = localtime($hash->{LASTUPDATE});# letzter Durchlauf 
   my (undef,$next_min,$next_hour,undef,$next_month,$next_year,$next_day) = localtime(time()+AttrNum($name,"interval",60)); # nächster Durchlauf 
 
   my $lastmin_run   = ($now_min  != $next_min)  ? 1 : 0;
   my $firstmin_run  = ($now_min  != $last_min)  ? 1 : 0;

   my $lasthour_run   = ($now_hour  != $next_hour)  ? 1 : 0;
   my $firsthour_run  = ($now_hour  != $last_hour)  ? 1 : 0;

   my $lastday_run    = ($now_day   != $next_day)   ? 1 : 0;
   my $firstday_run   = ($now_day   != $last_day)   ? 1 : 0;

   my $lastweek_run   = ($lastday_run  && ($now_day == 0)) ? 1 : 0;
   my $firstweek_run  = ($firstday_run && ($now_day == 1)) ? 1 : 0;

   my $lastmonth_run  = ($now_month != $next_month) ? 1 : 0;
   my $firstmonth_run = ($now_month != $last_month) ? 1 : 0;
   
   my $lastyear_run   = ($now_year  != $next_year)  ? 1 : 0;
   my $firstyear_run  = ($now_year  != $last_year)  ? 1 : 0;

   my $in_wirk     = hex(substr($hex,64,8))/10000;
   my $in_wirk_c   = int(hex(substr($hex,80,16))/3600000);
   my $in_wirk_cw  = int(hex(substr($hex,80,16))/3600);
   my $out_wirk    = hex(substr($hex,104,8))/10000;
   my $out_wirk_c  = int(hex(substr($hex,120,16))/3600000);
   my $out_wirk_cw = int(hex(substr($hex,120,16))/3600);

   my $in_today   = ($in_wirk_c  - ReadingsNum($name,"In_Start_Day",   0));
   my $out_today  = ($out_wirk_c - ReadingsNum($name,"Out_Start_Day",  0));

   my $verbrauch;
   my $ertrag;
   my $ertrag_w; 
   my $ertrag_kw;

   $ertrag_w   = (AttrVal($name,'inverter1','')) ? ReadingsNum(AttrVal($name,'inverter1',''),'Gesamtertrag_Wh' ,0) : 0;
   $ertrag_kw  = (AttrVal($name,'inverter1','')) ? ReadingsNum(AttrVal($name,'inverter1',''),'Gesamtertrag_KWh',0) : 0;
   $ertrag_w  += (AttrVal($name,'inverter2','')) ? ReadingsNum(AttrVal($name,'inverter2',''),'Gesamtertrag_Wh' ,0) : 0;
   $ertrag_kw += (AttrVal($name,'inverter2','')) ? ReadingsNum(AttrVal($name,'inverter2',''),'Gesamtertrag_KWh',0) : 0;

  readingsBeginUpdate($hash);
  

   if ($lastmin_run)
    {
      my $im = ($in_wirk_cw   - ReadingsNum($name,"In_Start_Min",  0));
      my $om = ($out_wirk_cw  - ReadingsNum($name,"Out_Start_Min", 0));
      readingsBulkUpdate($hash, "In_Start_Min",  $in_wirk_cw);
      readingsBulkUpdate($hash, "Out_Start_Min", $out_wirk_cw);

      $ertrag = ($ertrag_w - ReadingsNum($name,'Ertrag_Start_Hour',0));
      $verbrauch = ($im + $ertrag - $om);
      readingsBulkUpdate($hash,'Verbrauch_Min', $verbrauch);
      readingsBulkUpdate($hash,'Verbrauch', (ReadingsNum($name,'Verbrauch',0)+$verbrauch)) if ($verbrauch >0);
    }


    if ($lasthour_run)
    {
      my $ih = ($in_wirk_cw   - ReadingsNum($name,"In_Start_Hour",  0));
      my $oh = ($out_wirk_cw  - ReadingsNum($name,"Out_Start_Hour", 0));

      readingsBulkUpdate($hash, "In_Hour",  $ih);
      readingsBulkUpdate($hash, "Out_Hour", $oh);
      readingsBulkUpdate($hash, "In_Start_Hour",  $in_wirk_cw);
      readingsBulkUpdate($hash, "Out_Start_Hour", $out_wirk_cw);
     
      $ertrag = ($ertrag_w - ReadingsNum($name,'Ertrag_Start_Hour',0));
      readingsBulkUpdate($hash,'Ertrag_Hour',       $ertrag);
      readingsBulkUpdate($hash,'Ertrag_Start_Hour', $ertrag_w);

      #$verbrauch = ($ih + $ertrag - $oh);
      #readingsBulkUpdate($hash,'Verbrauch_Hour', $verbrauch);
      #readingsBulkUpdate($hash,'Verbrauch', (ReadingsNum($name,'Verbrauch',0)+$verbrauch));
    }

    #if ($firsthour_run)
    #{
    #}


    if ($lastday_run) # letzter Lauf für heute ?
    {
     #Log3 $hash, 3 ,"$name, lastday run: $lastweek_run, $lastmonth_run, $lastyear_run";
     readingsBulkUpdate($hash,"lastdayrun",$lastweek_run.$lastmonth_run.$lastyear_run);
     readingsBulkUpdate($hash, "In_Day",   $in_today);
     readingsBulkUpdate($hash, "Out_Day",  $out_today);
     readingsBulkUpdate($hash, "OI_Day",   int($out_today / $in_today * 100))  if ($in_today != 0);
     readingsBulkUpdate($hash, "OI_Total", int($out_wirk_c / $in_wirk_c * 100))  if ($in_wirk_c);

     $ertrag = ($ertrag_w - ReadingsNum($name,'Ertrag_Start_Day',0));
     readingsBulkUpdate($hash,'Ertrag_Day',$ertrag);

     $verbrauch = ($in_today + int($ertrag/1000) - $out_today);
     readingsBulkUpdate($hash,'Verbrauch_Day',$verbrauch);


     if ($lastweek_run) # letzter Lauf für diese Woche ?
     {
      my $iw = ($in_wirk_c  - ReadingsNum($name,"In_Start_Week",  0));
      my $ow = ($out_wirk_c - ReadingsNum($name,"Out_Start_Week", 0));
      readingsBulkUpdate($hash, "In_Week",  $iw);
      readingsBulkUpdate($hash, "Out_Week", $ow);
      readingsBulkUpdate($hash, "OI_Week",  int($ow / $iw * 100)) if($iw);
      
      $ertrag = ($ertrag_kw - ReadingsNum($name,'Ertrag_Start_Week',0));
      readingsBulkUpdate($hash,'Ertrag_Week',$ertrag);

      $verbrauch = ($iw + $ertrag - $ow);
      readingsBulkUpdate($hash,'Verbrauch_Week',$verbrauch);

     }
     if ($lastmonth_run) # letzter Lauf für diesen Monat ?
     {
      my $im = ($in_wirk_c  - ReadingsNum($name,"In_Start_Month",  0));
      my $om = ($out_wirk_c - ReadingsNum($name,"Out_Start_Month", 0));
      readingsBulkUpdate($hash, "In_Month", $im);
      readingsBulkUpdate($hash, "Out_Month",$om);
      readingsBulkUpdate($hash, "OI_Month", int($om / $im * 100)) if($im);

      $ertrag = ($ertrag_kw - ReadingsNum($name,'Ertrag_Start_Month',0));
      readingsBulkUpdate($hash,'Ertrag_Month',$ertrag);

      $verbrauch = ($im + $ertrag - $om);
      readingsBulkUpdate($hash,'Verbrauch_Month',$verbrauch);
     }
     if ($lastyear_run) # letzter Lauf für dieses Jahr ?
     {
      my $iy = ($in_wirk_c  - ReadingsNum($name,"In_Start_Year",  0));
      my $oy = ($out_wirk_c - ReadingsNum($name,"Out_Start_Year", 0));
      readingsBulkUpdate($hash, "In_Year",  $iy);
      readingsBulkUpdate($hash, "Out_Year", $oy);
      readingsBulkUpdate($hash, "OI_Year",  int($oy / $iy * 100)) if($iy);

      $ertrag = ($ertrag_kw - ReadingsNum($name,'Ertrag_Start_Year',0));
      readingsBulkUpdate($hash,'Ertrag_Year',$ertrag_kw);
    
      $verbrauch = ($iy + $ertrag - $oy);
      readingsBulkUpdate($hash,'Verbrauch_Year',$verbrauch);
     }
    }
    elsif ($firstday_run) # erster Lauf für heute ?
    {
     #Log3 $hash, 3 ,"$name, firstday run: $firstweek_run, $firstmonth_run, $firstyear_run";
     readingsBulkUpdate($hash,"firstdayrun",    $firstweek_run.$firstmonth_run.$firstyear_run);
     readingsBulkUpdate($hash, "In_Start_Day",  $in_wirk_c);
     readingsBulkUpdate($hash, "Out_Start_Day", $out_wirk_c);
     readingsBulkUpdate($hash, "Diff_Wirk_Min", 0);
     readingsBulkUpdate($hash, "Diff_Wirk_Max", 0);
     readingsBulkUpdate($hash,'Ertrag_Start_Day',$ertrag_w);

     if ($firstweek_run) # erster Lauf für diese Woche ?
     {
      readingsBulkUpdate($hash, "In_Start_Week",  $in_wirk_c);
      readingsBulkUpdate($hash, "Out_Start_Week", $out_wirk_c);
      readingsBulkUpdate($hash,'Ertrag_Start_Week',$ertrag_kw);
     }
     if ($firstmonth_run) # erster Lauf für diesen Monat ?
     {
      readingsBulkUpdate($hash, "In_Start_Month",  $in_wirk_c);
      readingsBulkUpdate($hash, "Out_Start_Month", $out_wirk_c);
      readingsBulkUpdate($hash,'Ertrag_Start_Month',$ertrag_kw);
     }
     if ($firstyear_run) # erster Lauf für dieses Jahr ?
     {
      readingsBulkUpdate($hash, "In_Start_Year",  $in_wirk_c);
      readingsBulkUpdate($hash, "Out_Start_Year", $out_wirk_c);
      readingsBulkUpdate($hash,'Ertrag_Start_Year',$ertrag_kw);
     }
    }
    else
    {
     my $d = $out_wirk - $in_wirk;
     my $DWav5 = sprintf("%.1f",SMA_HM_mAv($hash,"Diff_Wirk5", $d,300));
     $DWav5 =~ s/\.?0*$//;
     $DWav5 += 0;
     my $DWav2 = sprintf("%.2f",SMA_HM_mAv($hash,"Diff_Wirk2", $d,120));
     $DWav2 =~ s/\.?0*$//;
     $DWav2 += 0;
     my $DWav1 = sprintf("%.1f",SMA_HM_mAv($hash,"Diff_Wirk1", $d,60));
     $DWav1 =~ s/\.?0*$//;
     $DWav1 += 0;

     my $DW = sprintf("%.1f",$d);
     $DW =~ s/\.?0*$//;
     $DW += 0;

     $in_wirk = sprintf("%.1f",$in_wirk);
     $in_wirk =~ s/\.?0*$//;
     $in_wirk +=0;

     $out_wirk = sprintf("%.1f",$out_wirk);
     $out_wirk =~ s/\.?0*$//;
     $out_wirk += 0;
 
     my $trend_1 = '&#x25B4;&#x25BE;';
     my $trend_2 = '&#x25B4;&#x25BE;';
     my $trend_5 = '&#x25B4;&#x25BE;';
     my $trend   = $DWav1 - ReadingsNum($name,'Diff_Wirk_av1',0);
     $trend_1 = '&#x25B2'  if ($trend>0);
     $trend_1 = '&#x25BC;' if ($trend<0);

     $trend   = $DWav2 - ReadingsNum($name,'Diff_Wirk_av1',0);
     $trend_2 = '&#x25B2'  if ($trend>0);
     $trend_2 = '&#x25BC;' if ($trend<0);

     $trend   = $DWav5 - ReadingsNum($name,'Diff_Wirk_av5',0);
     $trend_5 = '&#x25B2'  if ($trend>0);
     $trend_5 = '&#x25BC;' if ($trend<0);

     #readingsBulkUpdate($hash, "state",       sprintf("%.2g",$out_wirk - $in_wirk)." kW / ". int($out_today / $in_today * 100). " %") if ($in_today);
     readingsBulkUpdate($hash, "Diff_Wirk",   $DW);
     readingsBulkUpdate($hash, "Diff_Wirk_c", ($out_wirk_c - $in_wirk_c));
     readingsBulkUpdate($hash, "In_Wirk",     $in_wirk);
     readingsBulkUpdate($hash, "In_Wirk_c",   $in_wirk_c);
     readingsBulkUpdate($hash, "Out_Wirk",    $out_wirk);
     readingsBulkUpdate($hash, "Out_Wirk_c",  $out_wirk_c);
     readingsBulkUpdate($hash, "Diff_Wirk_av5", $DWav5);
     readingsBulkUpdate($hash, "Diff_Wirk_av2", $DWav2);
     readingsBulkUpdate($hash, "Diff_Wirk_av1", $DWav1);
     readingsBulkUpdate($hash, "In_today",    $in_today);
     readingsBulkUpdate($hash, "Out_today",   $out_today);
     readingsBulkUpdate($hash, "Trend_1",     $trend_1);
     readingsBulkUpdate($hash, "Trend_2",     $trend_2);
     readingsBulkUpdate($hash, "Trend_5",     $trend_5);
     readingsBulkUpdate($hash, "Diff_Wirk_Max", $DW) if ($DW > ReadingsNum($name,'Diff_Wirk_Max',0));
     readingsBulkUpdate($hash, "Diff_Wirk_Min", $DW) if ($DW < ReadingsNum($name,'Diff_Wirk_Min',0));

     if ($in_today != 0)
     { readingsBulkUpdate($hash, "OI_today",  int($out_today / $in_today *100)); }
     else
     { readingsBulkUpdate($hash, "OI_today",  0); }
    }
 
    if (int(AttrVal($name,"detail-level","1")) > 1)
    {

     readingsBulkUpdate($hash, "In_Blind",    sprintf("%.2f",hex(substr($hex,144,8))/10000));
     readingsBulkUpdate($hash, "In_Blind_c",  int(hex(substr($hex,160,16))/3600000));
     readingsBulkUpdate($hash, "Out_Blind",   sprintf("%.2f",hex(substr($hex,184,8))/10000));
     readingsBulkUpdate($hash, "Out_Blind_c", int(hex(substr($hex,200,16))/3600000));
     readingsBulkUpdate($hash, "In_Schein",   sprintf("%.2f",hex(substr($hex,224,8))/10000));
     readingsBulkUpdate($hash, "In_Schein_c", int(hex(substr($hex,240,16))/3600000));
     readingsBulkUpdate($hash, "Out_Schein",  sprintf("%.2f",hex(substr($hex,264,8))/10000));
     readingsBulkUpdate($hash, "Out_Schein_c",int(hex(substr($hex,280,16))/3600000));

    # Offsets L1 -L3
    my @of = (320,336,360,376,400,416,440,456,480,496,520,536,560,576,592);

    my $in_wirk    = "";
    my $in_wirk_c  = "";
    my $in_blind   = "";
    my $in_blind_c = "";
    my $in_schein  = "";
    my $in_schein_c= "";

    my $out_wirk    = "";
    my $out_wirk_c  = "";
    my $out_blind   = "";
    my $out_blind_c = "";
    my $out_schein  = "";
    my $out_schein_c= "";

    my $thd;
    my $v;
    my $cosphi;

    for (my $i=0;$i<3;$i++)
    {
    # L1 320 , L2 320+288 , L3 320+288+288

    $in_wirk     .= sprintf("%.2f",hex(substr($hex,$of[0]+$i*288,8))/10000)." ";
    $in_wirk_c   .= (hex(substr($hex,$of[1]+$i*288,16))/3600000)." ";
    $out_wirk     .= sprintf("%.2f",hex(substr($hex,$of[2]+$i*288,8))/10000). " ";
    $out_wirk_c   .= (hex(substr($hex,$of[3]+$i*288,16))/3600000)." ";
    $in_blind    .= sprintf("%.2f",hex(substr($hex,$of[4]+$i*288,8))/10000). " ";
    $in_blind_c  .= (hex(substr($hex,$of[5]+$i*288,16))/3600000)." ";
    $out_blind    .= sprintf("%.2f",hex(substr($hex,$of[6]+$i*288,8))/10000). " ";
    $out_blind_c  .= (hex(substr($hex,$of[7]+$i*288,16))/3600000)." ";
    $in_schein   .= sprintf("%.2f",hex(substr($hex,$of[8]+$i*288,8))/10000). " ";
    $in_schein_c .= (hex(substr($hex,$of[9]+$i*288,16))/3600000)." ";
    $out_schein   .= sprintf("%.2f",hex(substr($hex,$of[10]+$i*288,8))/10000). " ";
    $out_schein_c .= (hex(substr($hex,$of[11]+$i*288,16))/3600000)." ";
    $thd        .= int(hex(substr($hex,$of[12]+$i*288,8))/1000)." ";
    $v          .= int(hex(substr($hex,$of[13]+$i*288,8))/1000)." ";
    $cosphi     .= (hex(substr($hex,$of[14]+$i*288,8))/1000)." ";

    readingsBulkUpdate($hash, "S_In_Wirk",     $in_wirk);
    readingsBulkUpdate($hash, "S_In_Wirk_c",   $in_wirk_c);
    readingsBulkUpdate($hash, "S_Out_Wirk",    $out_wirk);
    readingsBulkUpdate($hash, "S_Out_Wirk_c",  $out_wirk_c);	
    readingsBulkUpdate($hash, "S_In_Blind",    $in_blind);
    readingsBulkUpdate($hash, "S_In_Blind_c",  $in_blind_c);
    readingsBulkUpdate($hash, "S_Out_Blind",   $out_blind);
    readingsBulkUpdate($hash, "S_Out_Blind_c", $out_blind_c);
    readingsBulkUpdate($hash, "S_In_Schein",   $in_schein);
    readingsBulkUpdate($hash, "S_In_Schein_c", $in_schein_c);
    readingsBulkUpdate($hash, "S_Out_Schein",  $out_schein);
    readingsBulkUpdate($hash, "S_Out_Schein_c",$out_schein_c);
    readingsBulkUpdate($hash, "S_THD",         $thd);
    readingsBulkUpdate($hash, "S_Volt",        $v);
    readingsBulkUpdate($hash, "S_CosPhi",      $cosphi);
    }
   }

    readingsEndUpdate($hash, 1); 
return undef; 
}

sub SMA_HM_mAv($$$$)
{
   my ($hash,$reading,$val,$avtime) = @_;
   my $now  = time();
   my @new  = ($val,$now);
    
   #-- test for existence
   if(!$hash->{HELPER}{$reading}{mAV})
   {
    push(@{$hash->{HELPER}{$reading}{mAV}},\@new);
    return $val;
   } 
 
   my $num = int(@{$hash->{HELPER}{$reading}{mAV}});
   my $arr = \@{$hash->{HELPER}{$reading}{mAV}};

   if( ($num < 30) && ( ($now - $arr->[0][1]) < $avtime) )
     { push(@{$hash->{HELPER}{$reading}{mAV}},\@new); }
     else
     {
      shift(@{$hash->{HELPER}{$reading}{mAV}});
      push(@{$hash->{HELPER}{$reading}{mAV}},\@new);
     }
   #-- output and average
   my $av = 0;
   for (my $i=0;$i<$num;$i++) { $av += $arr->[$i][0]; }
   return $av/$num;
 }

1;

