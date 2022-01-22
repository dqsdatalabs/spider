import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters

class EnvisionrealtySpider(scrapy.Spider):
    name = 'envisionrealty'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['listings.envisionrealty.ca']
    start_urls = ['http://listings.envisionrealty.ca/index.asp?PageAction=searchresult&SortOrder=listDate&searchDetails=Toronto&bedrooms=any&bathrooms=any&pricerangefrom=0&pricerangeto=0&Categories_ID=337530&sale_lease=Lease&garages=any&kitchens=0&pool=any&fbclid=IwAR0WBw43_rnEsZSLJwtpaOmzrL5qspaH2lNefxhThi5K7XZghw1yizkFj4w']


    def start_requests(self):
        base_url = 'http://listings.envisionrealty.ca/index.asp?PageAction=searchresult&SortOrder=listDate&searchDetails=Toronto&bedrooms=any&bathrooms=any&pricerangefrom=0&pricerangeto=0&Categories_ID=337530&sale_lease=Lease&garages=any&kitchens=0&pool=any&fbclid=IwAR0WBw43_rnEsZSLJwtpaOmzrL5qspaH2lNefxhThi5K7XZghw1yizkFj4w&Page='
        for i in range(1,393,1):
            yield scrapy.Request(url=base_url+str(i), callback=self.parse)


    def parse(self, response):
        for url in response.css(".property.clearfix a::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_page)


    def parse_page(self, response):

        rent = response.css("h2.details::text").re("\$[0-9]+,*[0-9]*")
        if len(rent)  != 0:

    
            title = response.css("h1.location::text").get()
            address = response.css("h1.location::text").get()
            if address:
                address, city = address.split(",")
            images = response.css("#ListingImages img::attr(src)").getall()
            room_count = response.css("h2.details::text").re("[0-9]+\W*Bed")
            bathroom_count = response.css("h2.details::text").re("[0-9]+\W*Bath")
            rent = int(float(rent[0].replace("$","").replace(",","").strip()))
            external_id=response.css("h2.details::text").re("#:[a-zA-Z0-9]+")[0].replace("#:","").strip()
            description = response.css(".content p::text").get()
            property_type = make_property_type(response.xpath('//tr/th[contains(text(), "Property Style")]/following-sibling::th/text()').get())
            square_meters = response.xpath('//tr/th[contains(text(), "Square Feet")]/following-sibling::th/text()').get()
            parking = response.xpath('//tr/th[contains(text(), "Garages")]/following-sibling::th/text()').get() not in ['0.0', ' N/A', 'None']
            swimming_pool = response.xpath('//tr/th[contains(text(), "Pool")]/following-sibling::th/text()').get()
            landlord_name = response.css(".head-contact h3::text").get()
            landlord_phone = response.css("a.contact-phone::attr(href)").get().replace("tel:","")
            landlord_email = response.css("a.contact-email::attr(href)").get().replace("mailto:","")
    
    
            if bathroom_count:
                bathroom_count = int(float(eval(bathroom_count[0].replace("Bath","").strip())))
    
            if room_count:
                room_count = int(float(eval(room_count[0].replace("Bed","").strip())))
    
            if square_meters:
                if '-' in square_meters:
                    square_meters = sq_feet_to_meters(int(eval(square_meters.replace("-","+"))/2))
                else:
                    square_meters = sq_feet_to_meters(int(square_meters.replace("<").strip()))
    
            if swimming_pool:
                if swimming_pool == "None":
                    swimming_pool = ''
                for false in ['0.0', 'N/A']:
                    if swimming_pool in false:
                        swimming_pool = False
                        break
                else:
                    swimming_pool = True
    
            
            
    
    
    
            balcony,diswasher,washing_machine = fetch_amenities(response.css(".resp-tabs-container .content p::text").getall())
    
    
    
            item = ListingLoader(response=response)
            item.add_value("external_source"        ,self.external_source)
            item.add_value("address"                ,address)
            item.add_value("external_link"          ,response.url)
            item.add_value("currency"               ,"CAD")
            item.add_value("title"                  ,title)
            item.add_value("rent"                   ,rent)
            item.add_value("images"                 ,images)
            item.add_value("external_id"            ,external_id)
            item.add_value("swimming_pool"          ,swimming_pool)
            item.add_value("square_meters"          ,int(int(square_meters)*10.764))
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            item.add_value("property_type"          ,property_type)
            item.add_value("parking"                ,parking)
            item.add_value("description"            ,description)
            item.add_value("landlord_phone"         ,landlord_phone)
            item.add_value("landlord_email"         ,landlord_email)
            item.add_value("landlord_name"          ,landlord_name)
            # item.add_value("longitude"              ,longitude)
            # item.add_value("latitude"               ,latitude)
            item.add_value("city"                   ,city)
            item.add_value("balcony"                ,balcony)
            item.add_value("dishwasher"             ,diswasher)
            item.add_value("washing_machine"        ,washing_machine)
    
    
            yield item.load_item()


def fetch_amenities(l):
    balcony,diswasher,washing_machine = '','',''
    
    for i in l:
        if 'balcon' in i.lower():
            balcony = True
        elif 'dishwasher' in i.lower():
            diswasher = True
        elif 'Washer' in i:
            washing_machine = True
    return balcony,diswasher,washing_machine


def make_property_type(word):
    apartments = ['condo', 'apartment', 'loft', 'backsplit']
    houses = ['house','storey', 'townhse', 'multi-level', 'bungalow', 'sidesplit']
    studios = ['bachelor', 'studio']


    if "other" in word.lower():
        return ""

    for apart in apartments:
        if apart in word.lower():
            return'apartment'
                  
    for house in houses:
        if  house in  word.lower() :
            return 'house'
    for studio in studios:
        if  studio in  word.lower() :
            return 'studio'
    return word
            
