drop database if exists board_games;
create database board_games;
use board_games;

create table board_game (
    board_game_id int primary key,
    game_name varchar(255),
    year_published int,
    min_players int,
    max_players int,
    avg_rating decimal(3, 2),
    ranking int
);

create table domain (
    domain_name_id int primary key,
    domain_name varchar(255)
);

create table board_game_domains (
    board_game_id int,
    domain_name_id int,
    primary key (board_game_id, domain_name_id),
    foreign key (board_game_id) references board_game(board_game_id),
    foreign key (domain_name_id) references domain(domain_name_id)
);

create table mechanics (
    mechanics_name_id int primary key,
    mechanics_name varchar(255)
);

create table board_games_mechanics (
    board_game_id int,
    mechanics_name_id int,
    primary key (board_game_id, mechanics_name_id),
    foreign key (board_game_id) references board_game(board_game_id),
    foreign key (mechanics_name_id) references mechanics(mechanics_name_id)
);
