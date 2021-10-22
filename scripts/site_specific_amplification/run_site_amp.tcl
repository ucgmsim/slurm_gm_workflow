if { $argc != 2 } {
    puts "The add.tcl script requires arguments."
    puts "For example, tclsh add.tcl event_name."
    puts "Please try again."
} else {
    set input_path [lindex $argv 0]
    puts "GM_path = $GM_path"

    set param_path [lindex $argv 1]
    puts "PARAM_PATH = $param_path"

    set output_dir [lindex $argv 2]
    puts "Output_path = $output_path"

}

# Load all the site specific parameters
source $param_path

#find all GMs and loop through them
#set gMotionName [string range $gMotion 15 end-15 ]
#set earthquake [string range $gMotion 15 end-24 ]
#set comp        [string range $gMotion end-17 end-15]
source site_amp_inner_loop.tcl
wipe


