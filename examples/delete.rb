# Delete all documents matching a condition

client[:restaurants].find('borough' => 'Manhattan').delete_many

# Delete one document matching a condition

client[:restaurants].find('borough' => 'Queens').delete_one

# Delete all documents in a collection

client[:restaurants].find.delete_many

# Drop a collection

client[:restaurants].drop
