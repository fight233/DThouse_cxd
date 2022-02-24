#!/user/bin/gnuplot
reset
set terminal png

set title filename font ",20"

set ylabel "Time in Seconds"

set xdata time
set timefmt "%Y%m%d"
set format x "%Y-%m-%d"
set xlabel "Date"

set style data linespoints

set terminal pngcairo size 1024,768 enhanced font 'Segoe UI, 10'
set output filename . '.png'
set datafile separator ','

set key reverse Left outside
set grid

# plot 'perftest-influx-report.csv'	using 1:2  title "InfluxDB Write", \
# 	""			using 1:3 title "InfluxDB Query case1", \
# 	""			using 1:4 title "InfluxDB Query case2", \
# 	""			using 1:5 title "InfluxDB Query case3", \
# 	""			using 1:6 title "InfluxDB Query case4"
# 
plot filename . '.csv' 	using 1:2  title "DThouse Write", \
	""			using 1:3 title "DThouse Query case1", \
	""			using 1:4 title "DThouse Query case2", \
	""			using 1:5 title "DThouse Query case3", \
	""			using 1:6 title "DThouse Query case4"
