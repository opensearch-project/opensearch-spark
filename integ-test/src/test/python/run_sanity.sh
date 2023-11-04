#!/bin/bash

# Set the environment variable for the OpenSearch URL
export OPENSEARCH_URL="http://ec2-52-39-211-201.us-west-2.compute.amazonaws.com:9200"


# ASCII Art for OpenSearch Spark
cat << "EOF"
      ______       _____                    _____                   _______                   _____          
     /  __  \     |\    \                  /\    \                 /::\    \                 /\    \         
    /   \/   /|    \\    \                /::\____\               /::::\    \               /::\____\        
   /         //      \\    \              /:::/    /              /::::::\    \             /:::/    /        
  /__/\      \\       \|    | __         /:::/    /              /::::::::\    \           /:::/    /         
  \   \ \     \ __     |    |/  \       /:::/    /              /:::/~~\:::\    \         /:::/    /          
   \   \ \    /  \    /            /\  /:::/    /              /:::/    \:::\    \       /:::/    /           
    \   \ \/    /   /            / /\/:::/    /               /:::/    / \:::\    \     /:::/    /            
     \   \ /    /   /__          / / /:::/____/               /:::/____/   \:::\____\   /:::/    /             
      \___\|    |  |::::\______ / /  \:::\    \               |:::|    |     |:::|    | /:::/    /              
           \____|  |::::|    | | /    \:::\    \              |:::|____|     |:::|    |/:::/____/               
                 |  ~~ |    | | /      \:::\    \              \:::\    \   /:::/    / \:::\    \               
                 |     |    | | /        \:::\    \              \:::\    \ /:::/    /   \:::\    \              
                 |_____|    |_|/          \:::\____\              \:::\    /:::/    /     \:::\    \             
                                            \::/    /               \:::\__/:::/    /       \:::\    \            
                                             \/____/                 \::::::::/    /         \:::\    \           
                                                                  ~~\::::::/    /           \:::\    \          
                                                                     \::::/    /             \:::\____\         
                                                                      \::/____/               \::/    /         
                                                                        ~~                    \/____/          
EOF

# Check for command-line arguments
run_tables=""
run_queries=""
date_arg=""

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --run-tables) run_tables="--run-tables" ;;
        --run-queries) run_queries="--run-queries" ;;
        --use-date) date_arg="--use-date" ; shift; date_value="$1" ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

# Run the Python script with the parsed arguments
python sanity.py $run_tables $run_queries $date_arg $date_value