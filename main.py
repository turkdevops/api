import functools
from flask import jsonify, request, abort
from sqlalchemy import func, or_, select
from werkzeug.exceptions import HTTPException

from app import app
from models import db, HadithCollection, Book, Chapter, Hadith


@app.before_request
def verify_secret():
    if not app.debug and request.headers.get("x-aws-secret") != app.config["AWS_SECRET"]:
        abort(401)


@app.errorhandler(HTTPException)
def jsonify_http_error(error):
    response = {"error": {"details": error.description, "code": error.code}}

    return jsonify(response), error.code


def paginate_results(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        limit = int(request.args.get("limit", 50))
        page = int(request.args.get("page", 1))

        stmt = f(*args, **kwargs)
        queryset = db.paginate(stmt, page=page, per_page=limit, max_per_page=100)
        result = {
            "data": [x.serialize() for x in queryset.items],
            "total": queryset.total,
            "limit": queryset.per_page,
            "previous": queryset.prev_num,
            "next": queryset.next_num,
        }
        return jsonify(result)

    return decorated_function


def single_resource(f):
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        stmt = f(*args, **kwargs)
        result = db.first_or_404(stmt)
        result = result.serialize()
        return jsonify(result)

    return decorated_function


@app.route("/", methods=["GET"])
def home():
    return "<h1>Welcome to sunnah.com API.</h1>"


@app.route("/v1/collections", methods=["GET"])
@paginate_results
def api_collections():
    return select(HadithCollection).order_by(HadithCollection.collectionID)


@app.route("/v1/collections/<string:name>", methods=["GET"])
@single_resource
def api_collection(name):
    return select(HadithCollection).where(HadithCollection.name == name)


@app.route("/v1/collections/<string:name>/books", methods=["GET"])
@paginate_results
def api_collection_books(name):
    return (
        select(Book)
        .where(Book.collection == name, Book.status == 4)
        .order_by(func.abs(Book.ourBookID))
    )


@app.route("/v1/collections/<string:name>/books/<string:bookNumber>", methods=["GET"])
@single_resource
def api_collection_book(name, bookNumber):
    book_id = Book.get_id_from_number(bookNumber)
    return select(Book).where(
        Book.collection == name, Book.status == 4, Book.ourBookID == book_id
    )


@app.route("/v1/collections/<string:collection_name>/books/<string:bookNumber>/hadiths", methods=["GET"])
@paginate_results
def api_collection_book_hadiths(collection_name, bookNumber):
    return (
        select(Hadith)
        .where(Hadith.collection == collection_name, Hadith.bookNumber == bookNumber)
        .order_by(Hadith.englishURN)
    )


@app.route("/v1/collections/<string:collection_name>/hadiths/<string:hadithNumber>", methods=["GET"])
@single_resource
def api_collection_hadith(collection_name, hadithNumber):
    return select(Hadith).where(
        Hadith.collection == collection_name, Hadith.hadithNumber == hadithNumber
    )


@app.route("/v1/collections/<string:collection_name>/books/<string:bookNumber>/chapters", methods=["GET"])
@paginate_results
def api_collection_book_chapters(collection_name, bookNumber):
    book_id = Book.get_id_from_number(bookNumber)
    return (
        select(Chapter)
        .where(Chapter.collection == collection_name, Chapter.arabicBookID == book_id)
        .order_by(Chapter.babID)
    )


@app.route("/v1/collections/<string:collection_name>/books/<string:bookNumber>/chapters/<float:chapterId>", methods=["GET"])
@single_resource
def api_collection_book_chapter(collection_name, bookNumber, chapterId):
    book_id = Book.get_id_from_number(bookNumber)
    return select(Chapter).where(
        Chapter.collection == collection_name,
        Chapter.arabicBookID == book_id,
        Chapter.babID == chapterId,
    )


@app.route("/v1/hadiths", methods=["GET"])
@paginate_results
def api_hadiths():
    stmt = select(Hadith)

    # Apply filters based on query parameters
    collection = request.args.get("collection")
    if collection:
        stmt = stmt.where(Hadith.collection == collection)

    book_number = request.args.get("bookNumber")
    if book_number:
        stmt = stmt.where(Hadith.bookNumber == book_number)

    chapter_id = request.args.get("chapterId")
    if chapter_id:
        stmt = stmt.where(Hadith.babID == float(chapter_id))

    hadith_number = request.args.get("hadithNumber")
    if hadith_number:
        stmt = stmt.where(Hadith.hadithNumber == hadith_number)

    # Order by URN for consistent results
    return stmt.order_by(Hadith.englishURN)


@app.route("/v1/hadiths/<int:urn>", methods=["GET"])
@single_resource
def api_hadith(urn):
    return select(Hadith).where(or_(Hadith.arabicURN == urn, Hadith.englishURN == urn))

@app.route("/v1/hadiths/urns", methods=["GET"])
def api_hadiths_by_urns():
    # Enforce: urns must appear only once (no ?urns=1&urns=2)
    if len(request.args.getlist("urns")) != 1:
        abort(
            400,
            "Query parameter 'urns' must be provided exactly once. Example: ?urns=305,306",
        )

    urns_param = request.args.get("urns", "").strip()
    if not urns_param:
        abort(400, "Query parameter 'urns' is required. Example: ?urns=305,306")

    # Parse comma-separated URNs
    parts = [p.strip() for p in urns_param.split(",") if p.strip()]
    if not parts:
        abort(400, "Query parameter 'urns' is required. Example: ?urns=305,306")

    urns = []
    invalid = []
    seen = set()

    for p in parts:
        try:
            u = int(p)
        except (TypeError, ValueError):
            invalid.append(p)
            continue
        if u not in seen:
            seen.add(u)
            urns.append(u)

    if invalid:
        abort(400, f"Invalid URN(s): {', '.join(map(str, invalid))}")

    MAX_URNS = 100
    if len(urns) > MAX_URNS:
        abort(400, f"Too many URNs (max {MAX_URNS}).")

    results = (
        db.session.scalars(
            select(Hadith).where(
                or_(Hadith.englishURN.in_(urns), Hadith.arabicURN.in_(urns))
            )
        ).all()
    )

    by_eng = {h.englishURN: h for h in results}
    by_ar = {h.arabicURN: h for h in results}

    data = []
    missing = []
    for u in urns:
        h = by_eng.get(u) or by_ar.get(u)
        if h is None:
            missing.append(u)
        else:
            data.append(h.serialize())

    return jsonify({"count": len(data), "missing": missing, "data": data})


@app.route("/v1/hadiths/random", methods=["GET"])
@single_resource
def api_hadiths_random():
    # TODO Make this configurable instead of hardcoding
    return (
        select(Hadith)
        .where(Hadith.collection == "riyadussalihin")
        .order_by(func.rand())
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0")
