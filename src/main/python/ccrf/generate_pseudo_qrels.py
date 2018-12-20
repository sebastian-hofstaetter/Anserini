import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--rel", type=int, help='number of relevant', required=True)
    parser.add_argument("--non-rel", type=int, help='number of non-relevant', required=True)
    parser.add_argument("--run", type=str, help='run file', required=True)

    args = parser.parse_args()
    head = args.rel
    tail = args.non_rel
    run_file = args.run

    max_rank = {}

    with open(run_file, 'r') as f:
        for line in f:
            qid, _, docid, rank, _, _ = line.split(' ')
            max_rank[qid] = rank

    with open(run_file, 'r') as f:
        for line in f:
            qid, _, docid, rank, _, _ = line.split(' ')
            if int(rank) > head and int(rank) < int(max_rank[qid]) - tail:
                continue

            rel = 1 if int(rank) <= head else 0
            print(f'{qid} 0 {docid} {rel}')


