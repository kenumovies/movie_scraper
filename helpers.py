def call_db(table,args):
    urlparse.uses_netloc.append("postgres")
    url = urlparse.urlparse("postgres://cgzhugtkhkkipy:hImoFvY1K86iSY5rCcqjK40Yyt@ec2-54-204-20-164.compute-1.amazonaws.com:5432/d4q87lrmqau28e")

    con = psycopg2.connect(
      database=url.path[1:],
      user=url.username,
      password=url.password,
      host=url.hostname,
      port=url.port
    )

    with con:
        cur = con.cursor()
        cur.execute("INSERT INTO "+ table +" VALUES("+args+");")