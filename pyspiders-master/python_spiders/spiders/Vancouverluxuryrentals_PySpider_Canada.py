import scrapy
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters




class VancouverluxuryrentalsSpider(scrapy.Spider):
    name = 'vancouverluxuryrentals'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['vancouverluxuryrentals.com']
    start_urls = ['https://www.vancouverluxuryrentals.com/properties/featured']


    def parse(self, response):
        data = json.loads(response.body)


        for items in data['data']:
            if items['status'] != "Available":
                continue
            property_type = make_type(items['typeOfProperty'])
            item = ListingLoader(response=response)

            item.add_value("external_source", self.external_source)
            item.add_value("external_link"  , make_link(items['address'], items['propertyId']))
            item.add_value("external_id"    , str(items['propertyId']))
            item.add_value("images"         , items['imgs'])
            item.add_value("city"           , items['city'])
            item.add_value("address"        , items['address'])
            item.add_value("landlord_name"  , items['agent'])
            item.add_value("pets_allowed"   , items['allowPets'] != "No")
            item.add_value("available_date" , items['availableDate'])
            item.add_value("room_count"     , int(float(items['bedrooms'])))
            item.add_value("bathroom_count" , int(float(items['bathrooms'])))
            item.add_value("description"    , remove_white_spaces(items['details']))
            item.add_value("dishwasher"     , items['dishwasher'] != "No")
            item.add_value("furnished"      , items['furnished']!= "No")
            item.add_value("parking"        , items['numberOfParking']!="0")
            item.add_value("zipcode"        , items['postalCode'])
            item.add_value("rent"           , items['rent'])
            item.add_value("square_meters"  , int(int(sq_feet_to_meters(items['squareFeet']))*10.764))
            item.add_value("currency"       , "CAD")
            item.add_value("property_type"  , property_type)
            item.add_value("title"          , make_title(items['name'], items['address'], property_type))


            try:
                item.add_value("balcony"        , items['balcony'])
            except:
                item.add_value("balcony"        , False)

            try:
                item.add_value("swimming_pool"  , items['swimmingPool'])
            except:
                item.add_value("swimming_pool"  , False)            

            yield item.load_item()







def make_type(word):
    if word in ['Penthouse', 'Condo']:
        return 'apartment'
    if word.lower() in ['townhouse','house']:
        return 'house'
    return word.lower()


def make_title(word, address, property_type):
    if word in ["\"undefined\"", "\"N/A\""]:
        return property_type + " in " + address
    return word


def make_link(address, id):
    totalLink = "https://www.vancouverluxuryrentals.com/listing/"
    for c in address:
        if c in [',']:
            continue
        if c in [" ", "#"]:
            totalLink +="-"
        else:
            totalLink+=c

    return totalLink+"-property-id-"+str(id)
