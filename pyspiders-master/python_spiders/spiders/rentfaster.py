import scrapy
import requests
import json
import dateutil.parser
from ..loaders import ListingLoader
from python_spiders.helper import extract_location_from_coordinates, remove_white_spaces
import re

class RentfasterSpider(scrapy.Spider):
    name = 'rentfaster'
    execution_type = 'testing'
    country = 'canada'
    locale = 'ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['rentfaster.ca']
    start_urls = ['https://www.rentfaster.ca/api/map.json']
    position = 1

    def parse(self, response):
        res  = requests.get("https://www.rentfaster.ca/api/map.json")

        for ad in json.loads(res.text)['listings']:
            longitude, latitude     = str(ad['longitude']), str(ad['latitude'])
            zipcode, city, address  = extract_location_from_coordinates(longitude, latitude)
            dataUsage={
                "external_id"            :str(ad['ref_id']),
                "title"                  :ad['intro'],
                # "property_type"          :ad['type'],
                "landlord_phone"         :ad['phone'],
                "zipcode"                :zipcode,
                "city"                   :city,
                "address"                :address,
                "latitude"               :latitude,
                "longitude"              :longitude,
            }
            yield scrapy.Request(url=response.urljoin(ad['link']), callback=self.parse_page, meta={'dataUsage':dataUsage})

    def parse_page(self, response):
        description  = " ".join(response.css("#listingview_full_desc p ::text").getall())
        images       = response.css(".listing-img-wrapper a::attr(href)").re(".*\.jpg$")
        balcony,dishwasher,washing_machine, elevator, parking, pets_allowed, swimming_pool , terrace = fetch_amenities([i.strip() for i in response.css(".listing-feature::text").getall()])
        counter = 1
        ad = response.meta['dataUsage']
        title = ad['title']
        if not title:
            title = response.css("h1::text").get()
        for property in response.css(".multiunitline"):
            rent                = property.css('span[property="price"]::text').get()
            if rent:
                rent = int(rent)
            else:
                continue
            deposit             = property.css(".details-f-deposit::text").re("[0-9]+,*[0-9]*")
            property_type       = make_prop(property.css(".details-f-type .only-print::text").get())
            room_count          = property.css('meta[property="numberOfRooms"]::attr(value)').get()

            bathroom_count      = property.css(".details-f-baths::text").re('[0-9]+')
            square_meters       = property.css(".details-f-sq_feet::text").re('[0-9]+\W*ft')
            furnished           = property.css(".details-f-furnishing::text").get() not in  ["Unfurnished", None, ""]
            available_date      = property.css(".details-f-availability_date::text").get()
    
            if deposit:
                deposit             = int(deposit[0].replace("$","").replace(",",""))
            if room_count:
                room_count          = int(float(room_count))
            if bathroom_count:
                bathroom_count      = int(float(bathroom_count[0]))
            if square_meters:
                square_meters       = int(square_meters[0].replace("ft","").strip())
            if "No Vacancy" in available_date:
                continue
            if "Immediate" in available_date:
                available_date = ""
            else:
                available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")
            landlord_phone = ad['landlord_phone']
            if not landlord_phone:
                landlord_phone = '1-587-318-2876'
            description = re.sub("[Pp]lease* [Cc]all.*","",description)
            if not description:
                description = ", ".join([i.strip() for i in response.css(".listing-feature::text").getall()])

            if not room_count:
                room_count = 1
            item = ListingLoader(response=response)
            item.add_value("external_link"          ,response.url+"#"+str(counter))
            item.add_value("external_source"        ,self.external_source)
            item.add_value("external_id"            ,ad['external_id'])
            item.add_value("position"               ,self.position) # Int
            item.add_value("title"                  ,title)
            item.add_value("address"                ,ad['address'])
            item.add_value("zipcode"                ,ad['zipcode'])
            item.add_value("city"                   ,ad['city'])
            item.add_value("latitude"               ,ad['latitude'])
            item.add_value("longitude"              ,ad['longitude'])
            item.add_value("property_type"          ,property_type)
            item.add_value("square_meters"          ,square_meters)
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            item.add_value("description"            ,description)
            item.add_value("pets_allowed"           ,pets_allowed)
            item.add_value("available_date"         ,available_date)
            item.add_value("currency"               ,"CAD")
            item.add_value("parking"                ,parking)
            item.add_value("images"                 ,images)
            item.add_value("balcony"                ,balcony)
            item.add_value("elevator"               ,elevator)
            item.add_value("rent"                   ,rent)
            item.add_value("deposit"                ,deposit)
            item.add_value("dishwasher"             ,dishwasher)
            item.add_value("washing_machine"        ,washing_machine)
            item.add_value("swimming_pool"          ,swimming_pool)
            item.add_value("terrace"                ,terrace)
            item.add_value("landlord_phone"         ,landlord_phone)
            item.add_value("landlord_name"          ,'RentFaster')
            # item.add_value("landlord_email"         ,landlord_email)
            self.position += 1
            counter+=1
            if property_type.lower() not in ['mobile', 'parking', 'parking spot', 'storage'] and rent:
                yield item.load_item()

            


def fetch_amenities(l):
    balcony,diswasher,washing_machine, elevator, parking, pets_allowed, swimming_pool , terrace = '','','','','','','',''
    for i in l:
        if i:
            i = i.lower()
            if 'balcon' in i:
                balcony = True
            elif 'terrace' in i:
                terrace = True
            elif 'dishwasher' in i:
                diswasher = True
            
            elif 'washer' in i or 'laundry' in i:
                washing_machine = True
            
            elif 'parking' in i:
                parking = True
    
            elif 'elevator' in i:
                elevator = True
            elif "pets" in i:
                pets_allowed = True
            elif 'pool' in i:
                swimming_pool = True
    return balcony,diswasher,washing_machine, elevator, parking, pets_allowed, swimming_pool , terrace


def make_prop(val):
    apartments  = ['apartment', 'condo', '2-storey', 'shared', 'basement','plex', 'condo apt', '3-storey', 'condo townhouse', 'co-op apt','loft','bungaloft','2 1/2 storey', 'main floor']
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
    return val
