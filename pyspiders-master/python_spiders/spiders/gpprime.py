import scrapy
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
class GpprimeSpider(scrapy.Spider):
    name = 'gpprime'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['gpprime.net']
    start_urls = ['https://api.theliftsystem.com/v2/search?client_id=377&auth_token=sswpREkUtyeYjeoahA2i&city_id=2127&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=4000&only_available_suites=true&local_url_only=true&region=&keyword=false&property_types=&city_ids=1118%2C571%2C2127&ownership_types=&exclude_ownership_types=&amenities=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=1000&offset=0&count=false']
    def parse(self, response):
        data = json.loads(response.body)
        for items in data:
            if items['property_type'].lower() in ['land'] or items['availability_count'] == 0:
                continue
            square_meters = sq_feet_to_meters(items['statistics']["suites"]['square_feet']['average'])
            dataUsage={
                "external_source"   :self.external_source,
                "external_id"       :str(items['id']),
                "title"             :items['name'],
                "property_type"     :make_prop(items['property_type']),
                "landlord_email"    :items['contact']['email'],
                "landlord_phone"    :items['contact']['phone'],
                "address"           :items['address']['address'],
                "city"              :items['address']['city'],
                "zipcode"           :items['address']['postal_code'],
                "currency"          :'CAD',
                "pets_allowed"      :items['pet_friendly'],
                "parking"           :make_park(items['parking']),
                "longitude"         :items['geocode']['longitude'],
                "latitude"          :items['geocode']['latitude'],
                "rent"              :int(items['statistics']["suites"]['rates']['min']),

                "description"       :remove_white_spaces(items['details']['overview']),
            }
            yield scrapy.Request(url=items['permalink'],callback=self.parse_page, meta=dataUsage, dont_filter=True)
    def parse_page(self, response):
        external_source=response.meta["external_source"]
        external_id   =response.meta["external_id"]
        title    =response.meta["title"]
        property_type=response.meta["property_type"]
        landlord_email=response.meta["landlord_email"]
        landlord_phone=response.meta["landlord_phone"]
        address=response.meta["address"]
        city        =response.meta["city"]
        zipcode=response.meta["zipcode"]
        pets_allowed=response.meta["pets_allowed"]
        parking=response.meta["parking"]
        longitude=response.meta["longitude"]
        latitude=response.meta["latitude"]
        rent=response.meta["rent"]

        description=response.meta["description"]
        # images = response.css(".gallery-image div img::attr(src)").getall()
        i = 1
        for apart in response.css(".suites .suite"):
            furnished = ''
            if response.css(".suites .suite .availability div::text").get() == 'Available':
                title           = remove_white_spaces(apart.css("div .suite-type::text").get())
                rent            = int(float(remove_white_spaces(apart.css("div .suite-rate .value::text").get()).replace("$","")))
                images          = apart.css(".suite-photos .suite-photo::attr(href)").getall()
                description     = remove_white_spaces(apart.css(".suite-description p ::text").get())
                square_meters   = apart.css(".suite-sqft .value ::text").get()
                bathroom_count  = apart.css(".suite-bath .value::text").get()
                room_count      = apart.css(".suite-type::text").re('[0-9]+\W*[Bb]edroom')
                square_meters   = apart.css(".suite-sqft .value::text").get()

                # if square_meters:
                #     square_meters = sq_feet_to_meters(int(square_meters))

                if bathroom_count:
                    bathroom_count = int(float(bathroom_count))
                if 'bachelor' in title.lower():
                    room_count = 1
                    property_type = 'studio'
                else:
                    if room_count:
                        room_count = int(float(room_count[0].replace("Bedroom","").strip()))
                    else:
                        room_count = apart.css(".suite-description p ::text").re('[0-9]+\W*[Bb]edroom')
                        if room_count:
                            room_count = int(float(room_count[0].replace("Bedroom","").replace("bedroom","").strip()))
                item = ListingLoader(response=response)
                if square_meters != None:
                    square_meters = sq_feet_to_meters(int(square_meters.strip()))
                    item.add_value("square_meters",     int(int(square_meters)*10.764))
                else:
                    pass
                

                balcony, swimming_pool, washing_machine,elevator,parking, furnished = search_in_desc(description)
                balcony,dishwasher,washing_machine = fetch_amenities(response.css(".widget.amenities ul li ::text").getall())
                
                if 'furnished' in title.lower():
                    furnished = True
                external_link = response.url
                external_link = external_link+'#'+str(i)

                item.add_value("external_link",     external_link)
                item.add_value("title",             title)
                item.add_value("rent",              rent)
                item.add_value("images",            images)
                item.add_value("description",       description)
                item.add_value("furnished",         furnished)
                item.add_value("balcony",           balcony)
                item.add_value("dishwasher",        dishwasher)
                item.add_value("washing_machine",   washing_machine)
                item.add_value("external_source"        , external_source)
                item.add_value("external_id",       external_id)
                item.add_value("title",            title)
                item.add_value("property_type",     property_type)
                item.add_value("landlord_email",   landlord_email)
                item.add_value("landlord_phone",   landlord_phone)
                item.add_value("address",         address)
                item.add_value("city",           city)
                item.add_value("zipcode",        zipcode)
                item.add_value("currency",          'CAD')
                item.add_value("pets_allowed",    pets_allowed)
                item.add_value("parking",         parking)
                item.add_value("longitude",        longitude)
                item.add_value("latitude",      latitude)
                item.add_value("rent",           rent)
                item.add_value("square_meters",     int(int(square_meters)*10.764))
                item.add_value("room_count",        room_count)
                item.add_value("bathroom_count",   bathroom_count)
                item.add_value("description",     description)
                item.add_value("swimming_pool",     swimming_pool)
                item.add_value("elevator",     elevator)
                i+=1
                yield item.load_item()
def make_park(val):
    return val['indoor'] or val['outdoor'] or val['additional']
def make_prop(val):
    apartments  = ['apartment', 'duplex','fourplex']
    houses      = ['house']
    for house in houses:
        if house in val.lower():
            return 'house'
    for aprt in apartments:
        if aprt in val.lower():
            return 'apartment'
def fetch_amenities(l):
    balcony,diswasher,washing_machine = '','',''
    for i in l:
        if 'balcony' in i.lower():
            balcony = True
        elif 'dishwasher' in i.lower():
            diswasher = True
        elif 'Washer' in i:
            washing_machine = True
    return balcony,diswasher,washing_machine



def search_in_desc(desc):
    balcony, swimming_pool, washing_machine,elevator,parking, furnished  = '', '', '', '','',''
    desc = desc.lower()

    if 'laundry' in desc or 'washer' in desc:
        washing_machine = True

    if 'balcon' in desc:
        balcony = True

    if 'pool' in desc:
        swimming_pool = True

    if 'no pets' in desc:
        pets_allowed = False

    if 'elevator' in desc:
        elevator = True
    if 'furnished' in desc:
        furnished = True

    if 'parking' in desc or 'garage' in desc:
        parking = True


    return balcony, swimming_pool, washing_machine,elevator,parking, furnished

