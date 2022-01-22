import scrapy
import json
import requests
from ..loaders import ListingLoader
from python_spiders.helper import extract_location_from_coordinates


class RcpmsolutionsSpider(scrapy.Spider):
    name = 'rcpmsolutions'
    execution_type = 'testing'
    country = 'canada'
    locale = 'ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['app.tenantturner.com','rcpmsolutions.ca']
    start_urls = ['https://rcpmsolutions.ca/grande-prairie-rentals/']
    position = 1

    def parse(self, response):
        res = requests.get('https://app.tenantturner.com/listings-json/2999')       
        data = json.loads(res.text)
        for ad in data:
            room_count = int(float(ad['beds']))
            balcony, washing_machine, elevator,parking = search_in_desc(ad['description'])
            item = ListingLoader(response=response)
            item.add_value("external_link"          ,ad['btnUrl'])
            item.add_value("external_source"        ,self.external_source)
            item.add_value("external_id"            ,ad['id'])
            item.add_value("position"               ,self.position) # Int
            item.add_value("title"                  ,ad['title'])
            item.add_value("city"                   ,ad['city'])
            item.add_value("zipcode"                ,ad['zip'])
            item.add_value("address"                ,ad['address'])
            item.add_value("latitude"               ,str(ad['latitude']))
            item.add_value("longitude"              ,str(ad['longitude']))
            item.add_value("room_count"             ,room_count)
            item.add_value("pets_allowed"           ,ad['acceptPets']!='no pets')
            item.add_value("bathroom_count"         ,int(float(ad['baths'])))
            item.add_value("currency"               ,"CAD")
            item.add_value("description"            ,fix_desc(ad['description']))
            item.add_value("rent"                   ,int(ad['rentAmount']))
            item.add_value("available_date"         ,get_date(ad['dateAvailable'])) 
            item.add_value("balcony"                ,balcony)
            item.add_value("washing_machine"        ,washing_machine)
            item.add_value("elevator"               ,elevator)
            item.add_value("currency"               ,"CAD")
            item.add_value("parking"                ,parking)
            self.position += 1
            if room_count:
                yield scrapy.Request(url=ad['btnUrl'], callback=self.parse_page, meta={'item':item})
    def parse_page(self, response):
            images          = response.css(".mdc-layout-grid .rsImg::attr(src)").getall()
            square_meters   = int(response.css(".pre-qualify__rental-title::text").re("[0-9]+\W*sq")[0].replace("sq","").strip())
            deposit         = int(response.xpath('//th[contains(text(), "Deposit")]/following-sibling::td/text()').get().replace("$","").replace(",",""))
            item = response.meta['item']
            item.add_value("images"                 ,images)
            item.add_value("square_meters"          ,square_meters)
            item.add_value("property_type"          ,'apartment')
            yield item.load_item()

def get_date(date):
    if date == "Now":
        return ""
    else:
        return "-".join(date.split("/")[::-1])
def fix_desc(desc):
    if 'NOTE:' in desc:
        return desc.split("NOTE:")[0]

def search_in_desc(desc):
    balcony, washing_machine, elevator,parking = '', '', '',''
    desc = desc.lower()

    if 'laundr' in desc:
        washing_machine = True

    if 'balcon' in desc:
        balcony = True
    if 'elevator' in desc:
        elevator = True

    if 'parking' in desc or 'garage' in desc:
        parking = True



    return balcony, washing_machine, elevator, parking
