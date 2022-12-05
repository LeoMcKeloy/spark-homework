create table if not exists web (
    id int,
    timestamp bigint,
    type varchar(255),
    page_id int,
    tag varchar(255),
    sign boolean
);

create table if not exists lk (
    lk_id int,
    user_id int,
    fio varchar(255),
    birthday date,
    create_date date
    );

insert into web values
                            (12345, 1667627426, 'click', 101, 'Sport', false),
                            (12345, 1667627426, 'click', 101, 'Health', true),
                            (12347, 1667627234, 'visit', 101, 'Politic', false),
                            (12345, 1667627100, 'click', 102, 'Sport', true),
                            (12334, 1667627123, 'scroll', 102, 'Health', false),
                            (12346, 1667624555, 'click', 101, 'Politic', false),
                            (12346, 1667627429, 'visit', 98, 'Sport', true),
                            (12345, 1667620008, 'move', 100, 'Health', true);

insert into lk values
                      (1, 12345, 'Иванов Иван Иванович', '1987-04-13', '2021-03-25'),
                      (2, 12346, 'Петров Петр Петрович', '1989-05-23', '2022-01-15');