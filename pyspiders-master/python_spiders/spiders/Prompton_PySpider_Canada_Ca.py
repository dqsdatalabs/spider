import scrapy
from python_spiders.loaders import ListingLoader
from datetime import datetime



class PromptonSpider(scrapy.Spider):
    name = 'prompton_ca'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"

    allowed_domains = ['prompton.ca']
    start_urls = ['https://prompton.ca/status/for-rent/']

    def parse(self, response):
        for item in response.css("#grid div div a::attr(href)").getall():
            yield scrapy.Request(url=item, callback=self.parse_page)
    
    
    def parse_page(self, response):
        images              = get_images(response.css("div#content div div div div.slide-container div::attr(data-picture-url)").getall())
        title               = response.css("div.small-12.medium-6.large-7.columns h1::text").get()
        external_id         = response.xpath('//div/p[contains(text(), "MLS Number:")]/following-sibling::p').css("::text").get()
        rent                = int(float(response.xpath('//div/p[contains(text(), "Price:")]/following-sibling::p').css("::text").get().replace("$","").replace(",","")))
        address, city       = response.xpath('//div/p[contains(text(), "Address:")]/following-sibling::p').css("::text").getall()
        room_count          = make_rooms(response.xpath('//div/p[contains(text(), "Bedrooms:")]/following-sibling::p').css("::text").get())
        bathroom_count      = response.xpath('//div/p[contains(text(), "Bathrooms:")]/following-sibling::p').css("::text").get()
        parking             = response.xpath('//div/p[contains(text(), "Parking:")]/following-sibling::p').css("::text").get()
        property_type       = make_property(response.xpath('//div/p[contains(text(), "Property Type:")]/following-sibling::p').css("::text").get())
        square_meters       = get_square(response.xpath('//div/p[contains(text(), "Livng Area:")]/following-sibling::p').css("::text").re(r"[0-9]+"))
        landlord_name       = response.css("div.agent-details.row div .details p b::text").get()
        landlord_email      = response.css("div.agent-details.row div .details p a::text").get().strip()
        description         = response.css(".columns.small-12.medium-7.large-8.medium-push-5.large-push-4 p::text").getall()
        
        
        description,dishwasher, pets_allowed, washing_machine = make_desc(description)
        
        
        
        item = ListingLoader(response=response)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_link"          ,response.url)
        item.add_value("external_id"            ,external_id)
        item.add_value("title"                  ,title)
        item.add_value("address"                ,address)
        item.add_value("rent"                   ,rent)
        item.add_value("images"                 ,images)
        item.add_value("property_type"          ,property_type)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("square_meters"          ,int(int(square_meters)*10.764))
        item.add_value("parking"                ,parking == "yes")
        item.add_value("description"            ,description)
        item.add_value("currency"               ,"CAD")
        item.add_value("landlord_email"         ,landlord_email)
        item.add_value("landlord_name"          ,landlord_name)
        
        item.add_value("dishwasher"             ,dishwasher)
        item.add_value("pets_allowed"           ,pets_allowed)
        item.add_value("washing_machine"        ,washing_machine)
        
        
        yield item.load_item()


def get_images(images):
    newImages = []
    for image in images:
        newImages.append(image.replace("-150x150",""))
    return newImages


def make_rooms(text):
    if text == "Studio":
        return 1
    else:
        try:
            return eval(text)
        except:
            pass
    return text




def make_property(prop):
    if prop in ["Condo Apt",'Condo Townhouse']:
        return "apartment"
    return prop

def get_square(area):
    if len(area) > 0:
        area = area[0]
        return round(float(area)*0.0929)


def make_desc(desc):
    dishwasher = ""
    pets       = ""
    washer     = ""
    
    descs = " ".join(desc)
    
    if "dishwasher" in descs:
        dishwasher = True
    
    if "pets" in descs:
        pets = True
    
    if "washer" in descs:
        washer = True

    
    
    return desc, dishwasher, pets, washer

