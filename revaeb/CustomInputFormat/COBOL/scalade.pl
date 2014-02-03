$i = 0;
foreach my $arg (@ARGV) 
{
   $i = $i + 1;
   if ($i==1)
   {
     $tempCopy = $arg;
     $tempCopy =~ s/^\s+|\s+$//g;
    }
   if ($i==2)
   {
     $fileName = $arg;
     $fileName =~ s/^\s+|\s+$//g;
    }
   if ($i==3)
   {
     $fileNameOut = $arg;
     $fileNameOut =~ s/^\s+|\s+$//g;
    }
}

print 'Calculating the length of the copybook...' . "\n" ;

$GetCpyLen = 'perl /home/training/Desktop/prlscript/lencob.pl ' . $tempCopy;
$len = `$GetCpyLen`;

$len =~ s/^\s+|\s+$//g;

print 'Length of the copybook: ' . $len ."\n" ;


$pgmTemp = "/home/training/Desktop/prlscript/cblcall_record_temp";
$pgmOut = "/tmp/cblconvout" . time;
open(PGM,$pgmTemp);
open(CPY,$tempCopy);
open(PGMOUT,">$pgmOut");

@pgmLinesIn = <PGM>;
@cpyLines = <CPY>;

$ind = 0;
foreach $line(@cpyLines)
{
  push(@cpyOutLines,$line);
  @cpyOutLines[$ind] =~ s/ S9/ -9/; 
  @cpyOutLines[$ind] =~ s/\)V9/\).9/;
  @cpyOutLines[$ind] =~ s/ USAGE COMP-3././;
  @cpyOutLines[$ind] =~ s/ USAGE COMP././;    
  @cpyOutLines[$ind] =~ s/ USAGE COMP-3 / /;   
  @cpyOutLines[$ind] =~ s/ USAGE COMP / /;
  @cpyOutLines[$ind] =~ s/ COMP-3././;
  @cpyOutLines[$ind] =~ s/ COMP././;    
  @cpyOutLines[$ind] =~ s/ COMP-3 / /;   
  @cpyOutLines[$ind] =~ s/ COMP / /;


  if ((index(@cpyOutLines[$ind], ' X(') > -1) or (index(@cpyOutLines[$ind], ' X ') > -1))
  {    @words = split(' ', $line);
       push(@xvars,@words[1]);
   }
  $ind = $ind + 1;
}

print 'Input copybook: ' . "\n" ;
print @cpyLines;
print "\n";

print 'Output copybook: '  . "\n" ;
print @cpyOutLines;
print "\n";

print 'Preparing the Cobol program...'  . "\n";

$ind = 0;
foreach $line(@pgmLinesIn)
{
  $line =~ s/___len___/$len*2/ge;
  $line =~ s/___lenby2___/$len/; 
  if (index($line, '___input_copybook___') > -1) 
    {
      push(@pgmLinesOut, @cpyLines);
    } elsif (index($line, '___output_copybook___') > -1)
    {
      push(@pgmLinesOut, @cpyOutLines);
    } elsif (index($line, '___INPUT_VARIABLE___') > -1) 
    {
      foreach $xvar(@xvars)
	{ $newline = $line;
          $line =~ s/___INPUT_VARIABLE___/$xvar/;
          push(@pgmLinesOut,$line);
          $line = $newline;
	}
    } else 
    {
      push(@pgmLinesOut,$line);
    }
  $ind = $ind + 1;
}

print @pgmLinesOut;

print PGMOUT  @pgmLinesOut;
close(PGM);
close(PGMOUT);

$pgmOutExe = $pgmOut . '-exe ';
$CompileCmd = 'cobc -x -free -o ' . $pgmOutExe . $pgmOut;

print 'Compiling the Cobol program...'  . "\n";

system($CompileCmd);

$HadoopCmd = 'hadoop jar /usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.1.1.jar -D stream.io.identifier.resolver.class=com.ms.hadoop.BinaryBytesIdentifierResolver -libjars /home/training/workspace/CustomInputFormat/binarybytes.jar -io binarybytes -jobconf binary.input.format.length=' . $len . ' -input ' .  $fileName . ' -output ' . $fileNameOut . ' -mapper ' . $pgmOutExe . ' -reducer NONE -inputformat com.ms.hadoop.BinaryFixedLengthInputFormat -outputformat com.ms.hadoop.BinaryBytesOutputFormat;';

print 'Running the Hadoop Streaming Command: ' . $HadoopCmd . "\n";

system($HadoopCmd);

#$DeleteCmd = 'rm ' . $pgmOut . ' ' . $pgmOutExe . ' -f';
#system($DeleteCmd);
