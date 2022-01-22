import scrapy
from ..loaders import ListingLoader
import dateutil.parser
import json
class ImperraSpider(scrapy.Spider):
    name = 'imperra'
    execution_type = 'testing'
    country = 'canada'
    locale = 'ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['imperra.ca', 'api.theliftsystem.com']
    start_urls = ['https://api.theliftsystem.com/v2/search?locale=en&client_id=571&auth_token=sswpREkUtyeYjeoahA2i&city_id=2015&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=3300&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=3133%2C1978%2C1154%2C3396%2C2042%2C2081%2C2015&pet_friendly=&offset=0&count=false']
    position = 1

    def parse(self, response):
        data = json.loads(response.body)
        for ad in data:
            dataUsage={
            "external_id"            :str(ad['id']),
            "title"                  :ad['name'],
            "property_type"          :make_prop(ad['property_type']),
            "landlord_name"          :ad['contact']['name'],
            "landlord_phone"         :ad['contact']['phone'],
            "landlord_email"         :ad['contact']['email'],
            "city"                   :ad['address']['city'],
            "zipcode"                :ad['address']['postal_code'],
            "address"                :ad['address']['address'],
            "latitude"               :ad['geocode']['latitude'],
            "longitude"              :ad['geocode']['longitude'],
            "pets_allowed"           :ad['pet_friendly'] == True,
            }
            yield scrapy.Request(url=ad['permalink'], callback=self.parse_page, meta={'dataUsage':dataUsage})

    def parse_page(self, response):
        images          = response.css(".slickslider a::attr(href)").re(".*jpg$")
        balcony,dishwasher,washing_machine, elevator, parking, swimming_pool = fetch_amenities([i.strip() for  i in response.css(".amenity-holder::text").getall()])
        counter = 1
        ad = response.meta['dataUsage']
        for apart in response.css(".suite-row"):
            room_count      = int(float(apart.css(".info-block .info::text").getall()[0].strip()))
            property_type   = apart.css(".suite-type::text").get()
            if 'Bachelor' in property_type:
                room_count = 1
                property_type = 'studio'
            else:
                property_type = ad['property_type']
            bathroom_count  = int(float(apart.css(".info-block .info::text").getall()[1].strip()))
            rent            = int(apart.css(".info-block .info::text").getall()[2].strip())
            available_date  = apart.css(".availability-label::text").get()
            floor_plan_images   = apart.css("a.floorplan-link::attr(href)").getall()
            if available_date:
                if "Not Available" in available_date:
                    continue
                elif 'Available Now' in available_date:
                    available_date = ''
                else:
                    available_date = dateutil.parser.parse(available_date.strip()).strftime("%Y-%m-%d")
            landlord_name = ad['landlord_name']
            if not landlord_name:
                landlord_name = "IMPERRA GENERAL"
            item = ListingLoader(response=response)
            item.add_value("external_link"          ,response.url+"#"+str(counter))
            item.add_value("external_source"        ,self.external_source)
            item.add_value("external_id"            ,ad['external_id'])
            item.add_value("position"               ,self.position) # Int
            item.add_value("title"                  ,ad['title'])
            item.add_value("address"                ,ad['address'])
            item.add_value("zipcode"                ,ad['zipcode'])
            item.add_value("city"                   ,ad['city'])
            item.add_value("latitude"               ,ad['latitude'])
            item.add_value("longitude"              ,ad['longitude'])
            item.add_value("property_type"          ,property_type)
            item.add_value("floor_plan_images"      ,floor_plan_images)
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            # item.add_value("description"            ,description)
            item.add_value("pets_allowed"           ,ad['pets_allowed'])
            item.add_value("currency"               ,"CAD")
            item.add_value("parking"                ,parking)
            item.add_value("images"                 ,images)
            item.add_value("balcony"                ,balcony)
            item.add_value("elevator"               ,elevator)
            item.add_value("rent"                   ,rent)
            item.add_value("dishwasher"             ,dishwasher)
            item.add_value("washing_machine"        ,washing_machine)
            item.add_value("swimming_pool"          ,swimming_pool)
            item.add_value("landlord_phone"         ,ad['landlord_phone'])
            item.add_value("landlord_name"          ,landlord_name)
            item.add_value("landlord_email"         ,ad['landlord_email'])
            self.position += 1
            counter+=1
            yield item.load_item()

def fetch_amenities(l):
    balcony,dishwasher,washing_machine, elevator, parking, swimming_pool = '','','','','',''
    for i in l:
        if i:
            if not i:
                continue
            i = i.lower()
            if 'balcon' in i:
                balcony = True
            elif 'dishwasher' in i:
                diswasher = True
            elif 'washer' in i or 'laundry' in i:
                washing_machine = True
            elif 'parking' in i:
                parking = True
            elif 'elevator' in i:
                elevator = True
            elif 'pool' in i:
                swimming_pool = True
            elif 'pets allowed' in i:
                pets_allowed = True
            elif 'unfurnished' in i:
                furnished = False
            elif 'furnished' in i:
                furnished = True
    return balcony,dishwasher,washing_machine, elevator, parking, swimming_pool


def make_prop(val):
    apartments  = ['apartment', 'condo', '2-storey','fourplex', 'condo apt', '3-storey', 'condo townhouse', 'co-op apt','loft','bungaloft','2 1/2 storey']
    houses      = ['detached', 'house', 'twnhouse', 'townhouse','bungalow','multi-level']
    studios     = ['studio', 'bachelor']
    if not val:
        return ''
    val =  val.lower()

    for house in houses:
        if house in val:
            return 'house'
    for aprt in apartments:
        if aprt in val:
            return 'apartment'
    for studio in studios:
        if studio in val:
            return 'studio'
    