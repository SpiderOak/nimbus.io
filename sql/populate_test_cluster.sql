/*
load cluster and node tables from diy schema
*/
delete from diy.cluster;
insert into diy.cluster (name, timestamp) values ("node-sim-cluster", current_timestamp);

delete from diy.node;
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 1, "node-sim-01", "localhost", current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 2, "node-sim-02", "localhost", current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 3, "node-sim-03", "localhost", current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 4, "node-sim-04", "localhost", current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 5, "node-sim-05", "localhost", current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 6, "node-sim-06", "localhost", current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 7, "node-sim-07", "localhost", current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 8, "node-sim-08", "localhost", current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 9, "node-sim-09", "localhost", current_timestamp);
insert into diy.node (cluster_id, node_number_in_cluster, name, hostname, creation_time)
values ((select id from cluster), 10, "node-sim-10", "localhost", current_timestamp);

