import scrapy
import dateutil.parser
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
from python_spiders.loaders import ListingLoader
import re

class SandstonemanagementSpider(scrapy.Spider):
    name = 'kirklandlakeapartments_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['sandstonemanagement.managebuilding.com']
    start_urls = ['https://sandstonemanagement.managebuilding.com/Resident/PublicPages/Home.aspx?ReturnUrl=%2fResident%2fdefault.aspx']

    def parse(self, response):
        for link in response.css("td.address h3 a::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(link), callback=self.parse_page)
    
    
    def parse_page(self, response):
        landlord_phone      = response.css(".contactInformation font b::text").get().lower().replace("call","").strip()
        address, city, zipcode = response.css(".rentalInfo .address::text").get().split(",")
        title               = response.css(".rentalInfo .address::text").get()
        rent                = int(float(response.css(".rentalInfo .rent::text").get().replace("$","").replace(",","")))
        available_date      = dateutil.parser.parse(response.css(".rentalInfo .availableDate::text").get().replace("Available:","").strip()).strftime("%Y-%m-%d")
        description         = remove_white_spaces(" ".join(response.css(".rentalDescription .mainDescription::text").getall()))
        parking             = apart_fetures(response.css(".propertyFeatures .featuresList li::text").getall())
        deposit             = int(float(re.findall("[0-9]+,*[0-9]*\.*[0-9]*",response.css(".securityDeposit::text").get())[0].replace(",","")))
        room_count, bathroom_count = response.css(".rentalInfo .beds::text").get().split("-")
        room_count          = int(room_count.replace("Bed","").strip())
        bathroom_count      = int(bathroom_count.replace("Bath","").strip())
        description         = re.sub("Call Sandstone.*[1 business day||available apartments].", "", description)
        balcony, pets_allowed, washing_machine = make_fetures(response.css(".rentalFeatures .featuresList li::text").getall())


        zipcode = zipcode.replace("ON","").strip()
        city = city.strip()

        item = ListingLoader(response=response)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("address"                ,address)
        item.add_value("external_id"            ,response.css("script").re('"Id":[0-9]+')[0].replace('"Id":',""))
        item.add_value("external_link"          ,response.url)
        item.add_value("currency"               ,"CAD")
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("parking"                ,parking)
        item.add_value("pets_allowed"           ,pets_allowed)
        item.add_value("available_date"         ,available_date)
        item.add_value("description"            ,description)
        item.add_value("deposit"                ,deposit)
        item.add_value("balcony"                ,balcony)
        item.add_value("washing_machine"        ,washing_machine)
        item.add_value("property_type"          ,'apartment')
        item.add_value("landlord_phone"         ,landlord_phone)
        item.add_value("landlord_name"          ,'Sandstone Property Management')
        item.add_value("landlord_email"         ,'info@sandstonemanagement.ca')
        item.add_value("zipcode"                ,zipcode)
        item.add_value("city"                   ,city)

        images = response.urljoin(response.xpath('//li/a[contains(text(),"See more photos")]/@href').get())
        yield scrapy.Request(url=images, callback=self.get_images, meta={"item":item})

    def get_images(self, response):
        base = 'https://sandstonemanagement.managebuilding.com/Resident/api/public/files/download?fileName='
        images  = [ base + i for i in  response.css("script::text").re(r"[0-9a-zA-Z]+_406x539\.jpg")]
        
        item = response.meta['item']
        item.add_value("images"        ,images)
        yield  item.load_item()
        

def make_fetures(val):
    balcony, pets_allowed, washing_machine = '', '', ''
    for i in val:
        if 'balcon' in i.lower():
            balcony = True
        elif 'pet' in i.lower():
            pets_allowed = True
        
        elif 'washer' in i.lower() or 'laundry' in i.lower():
            washing_machine = True
    return balcony, pets_allowed, washing_machine


def apart_fetures(val):
    for i in val:
        if 'parking' in i.lower():
            return True
