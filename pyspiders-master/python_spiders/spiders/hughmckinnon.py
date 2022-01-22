import scrapy
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
import re
class HughmckinnonSpider(scrapy.Spider):
    name = 'hughmckinnon'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['hughmckinnon.com']
    start_urls = ['https://www.hughmckinnon.com/assets/data/rental-properties/listings/index.json']

    def parse(self, response):
        data = json.loads(response.body)
        data = data['data']['rentalListing']['edges']
        
        for item in data:
            yield scrapy.Request(url="https://www.hughmckinnon.com/rental-properties/"+str(item['node']['id']), callback=self.parse_page)
    
    
    def parse_page(self, response):
        full_images     = []
        title           = response.css(".container.pb-3 h1::text").get()
        rent            = int(response.css(".container.pb-3 h3::text").get().replace("$","").strip())
        external_id     = response.css(".container.pb-3 h3 span::text").get().replace("PID:","").strip()
        images          = response.css(".carousel-inner div .carousel-item img::attr(src)").getall()
        description     = " ".join(response.css("ul li p::text").getall())
        swimming_pool   = more_fetures(response.css("div.col-lg-4.col-md-6.col-sm-12 ::text").getall())
        landlord_phone  = response.xpath('//div/h5[contains(text(), "Phone Number")]/following-sibling::p/a').css(" ::text").getall()[0]
        landlord_email  = response.xpath('//div/h5[contains(text(), "Email Address")]/following-sibling::p/a').css(" ::text").get()
        landlord_name   = response.xpath('//div/h5[contains(text(), "Listing Agent")]/following-sibling::a/p').css(" ::text").get()
        city            = response.css("div.tab-pane::text").get().replace(',','').strip().split(" ")[-1]
        address         = response.css("div.tab-pane::text").get().replace(city,"").strip()
        zipcode         = response.css("div.tab-pane span::text").get().replace(',','').strip()
        energy_label    = response.css("p ::text").re("Classe\W*[Energetica]*\W*[0-9a-zA-Z]+")
        longitude,latitude = response.xpath("//iframe[contains(@src,'https://maps.google.com')]/@src").re("[0-9]+\.[0-9]+,\-[0-9]+\.[0-9]+")[0].split(",")


        if energy_label:
            energy_label = energy_label[0].replace("Classe","").replace("Energetica","").strip()


        

        balcony, swimming_pool, washing_machine = search_in_desc(description)
        
        square_meters, room_count,bathroom_count, property_type, parking, pets_allowed, available_date   = get_features(response.css("div.col-md-4.col-sm-6 ul li ::text").getall())
        
        description = remove_white_spaces(description[:description.find("Lease Required")]+"Lease Required")

        if 'bachelor' in title.lower():
            room_count = 1
            property_type = 'studio'

        for image in images:
            full_images.append(response.urljoin(image))
        
        item = ListingLoader(response=response)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("address"                ,address)
        item.add_value("external_link"          ,response.url)
        item.add_value("currency"               ,"CAD")
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("images"                 ,full_images)
        item.add_value("external_id"            ,external_id)
        item.add_value("swimming_pool"          ,swimming_pool)
        item.add_value("square_meters"          ,int(int(square_meters)*10.764))
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("property_type"          ,property_type)
        item.add_value("parking"                ,parking)
        item.add_value("pets_allowed"           ,pets_allowed)
        item.add_value("available_date"         ,available_date)
        item.add_value("description"            ,description)
        item.add_value("landlord_phone"         ,landlord_phone)
        item.add_value("landlord_email"         ,landlord_email)
        item.add_value("landlord_name"          ,landlord_name)
        item.add_value("zipcode"                ,zipcode)
        item.add_value("longitude"              ,longitude)
        item.add_value("latitude"               ,latitude)
        item.add_value("city"                   ,city)
        item.add_value("balcony"                ,balcony)
        item.add_value("swimming_pool"          ,swimming_pool)
        item.add_value("washing_machine"        ,washing_machine)


        yield item.load_item()
            


def get_features(f):
    houses = ['townhouse', 'house']
    apartments = ['suite', 'condo']
    
    sq      = sq_feet_to_meters(int(f[0].replace("Sq Ft:","").strip()))
    beds    = f[1].replace("Bedroom","").replace("s","").strip()
    baths   = int(float(f[2].replace("Bathroom","").replace("s","").strip()))
    prop    = f[3].strip().lower()
    parking = f[4].strip() != 'None'
    pets    = ""
    available_date = ''
    for i in f:
        if "Available From" in i:
            available_date = i.lower()
            break
    
    for item in f:
        if "no pet" in item.lower():
            pets = False
            break
        elif "pet" in item.lower():
            pets = True
            break
    
    
    if available_date == "":
        available_date = ""
    else:
        available_date = available_date.replace("available from:", "").strip()
    
    
    if prop in houses:
        prop = 'house'
    elif prop in apartments:
        prop = 'apartment'
    
    
    if beds == "Studio":
        beds = 1
    elif beds == 'Murphy Bed':
        beds = ''
    else:
        beds    = int(float(beds))
    
    return sq, beds, baths, prop, parking, pets, available_date
    
    
    
    


def more_fetures(f):
    if len(f) > 0:
        # swimming_pool = ''
        for item in f:
            if item.strip().lower() == "swimming pool":
                return True
    return ''

def search_in_desc(desc):
    balcony, swimming_pool, washing_machine = '', '', ''

    if 'laundry'in desc or 'washer' in desc:
        washing_machine = True

    if 'balcon' in desc:
        balcony = True
    if 'pool' in desc:
        swimming_pool = True


    return balcony, swimming_pool, washing_machine

