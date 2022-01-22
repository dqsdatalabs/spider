import scrapy
from python_spiders.loaders import ListingLoader
import re
class AkmanmanagementSpider(scrapy.Spider):
    name = 'akmanmanagement'
    execution_type = 'testing'
    country = 'Canada'
    locale = 'Ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['akmanmanagement.ca']
    start_urls = ['https://www.akmanmanagement.ca/rentals/']

    def parse(self, response):
        for page in response.css(".h-filter-results").re("https://www.akmanmanagement.ca/properties/.*/"):
            yield scrapy.Request(url=page, callback=self.parse_page)


    def parse_page(self, response):
        
        images          = response.css(".x-slides li img::attr(src)").getall()
        title           = response.css(".prop-title span::text").get()    
        address         = response.css(".prop-address ::text").get()
        description     = fix_desc(response.css(".prop-single-container .prop-single-display-row .prop-single-container::text").getall())
        property_type   = get_type(" ".join(response.css(".prop-single-container .prop-single-display-row .prop-single-container::text").getall()))
        currency        = "USD"
        
        
        heads = response.css(".prop-single-head ::text").getall()
        for i in heads :
            i =  i.strip()
        iteration = 1
        if not "Units Available" in heads:
            availability = "".join(response.css(".h-single-prop-unit-avail-single-info ::text").getall())

            availability = re.findall("[0-9] Bed.*\n.*",availability)
            iteration = len(availability)
        
            for aprt in range(iteration):

                rent, square_meters, deposit, room_count, bathroom_count  = make_data(availability[aprt])

                item = ListingLoader(response=response)     

                item.add_value("external_source"        ,self.external_source)
                item.add_value("external_link"          ,response.url)
                item.add_value("images"                 ,images)
                item.add_value("title"                  ,title)
                item.add_value("address"                ,address)
                item.add_value("rent"                   ,rent)
                item.add_value("property_type"          ,property_type)
                item.add_value("room_count"             ,room_count)
                item.add_value("bathroom_count"         ,bathroom_count)
                item.add_value("square_meters"          ,int(int(square_meters)*10.764))
                item.add_value("description"            ,description)
                item.add_value("currency"               ,currency)
                item.add_value("deposit"                ,deposit)
                item.add_value("landlord_email"         ,'DOman@akmanmanagement.ca')
                item.add_value("landlord_phone"         ,'204.944.9721')


                yield item.load_item()
                
def get_type(data):
    l  = ['condo', 'apartment']
    for i  in l:
        if i in data:
            return "apartment"

    return i



def make_data(data):
    if "No Units" in data:
        return "", "", "", ""
    rooms    = re.findall("[0-9] Bedroom", data)
    baths    = re.findall("[0-9] Bathroom", data)
    rent     = re.findall("\$[0-9]+,*[0-9]*/Month", data)
    deposit  = re.findall("\$[0-9]+,*[0-9]* deposit", data)
    square   = re.findall("[0-9]+,*[0-9]* sq.ft.", data)
    
    if len(square) > 0:
        square = round(float(square[0].replace("sq.ft.","").replace(",","").strip())*.0929)
    if len(rooms) > 0:
        rooms = rooms[0].replace("Bedroom","").strip()
    if len(baths) > 0:
        baths = baths[0].replace("Bathroom","").strip()
    if len(rent) > 0:
        rent = int(rent[0].replace("$","").replace("/Month","").replace(",","").strip())
    if len(deposit) > 0:
        deposit = int(deposit[0].replace("$","").replace("deposit","").replace(",","").strip())
    
    
    

    return rent, square, deposit, rooms, baths


def fix_desc(desc):
    str_desc = ''
    for line in desc:
        str_desc += line.strip()
    return re.sub("[Pp]lease* contact.*\n*.*","",str_desc).replace("Akman Management204.944.9721","").replace("To book a viewing or request tenant services","").replace("To book a viewing","")