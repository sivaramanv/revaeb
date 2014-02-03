$i = 0;
foreach my $arg (@ARGV) {
   $i = $i + 1;
   if ($i==1)
   {
     $tempcopy = $arg;
     $tempcopy =~ s/^\s+|\s+$//g;
    }
}


$file = "/home/training/Desktop/prlscript/lencob";
$fileOut = "/home/training/Desktop/prlscript/lencobout" . time;
open(CPY,$file);
open(CPYOUT,">$fileOut");
@lines = <CPY>;
$ind = 0;
foreach $line(@lines)
{
  $ind = $ind + 1;
  if ($ind==6)
  {
    $line = "   COPY \"$tempcopy\".\n";
  }
}

print CPYOUT  @lines;
close(CPY);
close(CPYOUT);
$fileOutExe = $fileOut . '-exe ';
$CompileCmd = 'cobc -x -free -o ' . $fileOutExe . $fileOut;
$ExecuteCmd = './' . $fileOutExe;

system($CompileCmd);
system($ExecuteCmd);

$DeleteCmd = 'rm ' . $fileOut . ' ' . $fileOutExe . ' -f';
system($DeleteCmd);

