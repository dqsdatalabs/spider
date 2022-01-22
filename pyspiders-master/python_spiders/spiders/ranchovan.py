import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import extract_location_from_coordinates, remove_white_spaces

class RanchovanSpider(scrapy.Spider):
    name = 'ranchovan'
    execution_type = 'testing'
    country = 'canada'
    locale = 'ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['ranchovan.com']
    start_urls = ['https://www.ranchovan.com/rental-listings/']
    position = 1

    def parse(self, response):
        for url in response.css(".es-thumbnail a::attr(href)").getall():
            yield scrapy.Request(url=url, callback=self.parse_page)

        next_page = response.css("a.next.page-numbers::attr(href)").get()
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse)

    def parse_page(self, response):

        title           = response.css(".entry-title.fusion-post-title::text").get()
        rent            = int(response.css(".es-price::text").get().split(".")[0].replace("$","").replace(",","").strip())
        address         = response.css(".es-address::text").get()
        room_count      = response.xpath('//li/strong[contains(text(), "Bedrooms:")]/following-sibling::text()').get()
        bathroom_count  = int(float(response.xpath('//li/strong[contains(text(), "Bathrooms")]/following-sibling::text()').get().strip()))
        floor           = response.xpath('//li/strong[contains(text(), "Floors")]/following-sibling::text()').get()
        square_meters   = int(response.xpath('//li/strong[contains(text(), "Area")]/following-sibling::text()').re("[0-9]+,*[0-9]*")[0])
        property_type   = make_prop(response.xpath('//li/strong[contains(text(), "Type:")]/following-sibling::a/text()').get())
        available_date  = response.xpath('//li/strong[contains(text(), "Availability")]/following-sibling::text()').get()
        landlord_name   = response.xpath('//li/strong[contains(text(), "Agent Name")]/following-sibling::text()').get().strip()
        landlord_email  = response.xpath('//li/strong[contains(text(), "Agent Email")]/following-sibling::a/@href').get()
        landlord_phone  = response.xpath('//li/strong[contains(text(), "Agent Phone")]/following-sibling::text()').get()
        description     = remove_white_spaces(" ".join(response.css(".es-description p::text").getall()))
        latitude        = response.css("#es-google-map::attr(data-lat)").get()
        longitude       = response.css("#es-google-map::attr(data-lon)").get()
        external_id     = response.css("a.js-es-wishlist-button::attr(data-id)").get()
        images          = response.css(".es-gallery-image a::attr(href)").getall()
        balcony,diswasher,washing_machine, elevator, parking, pets_allowed, swimming_pool = fetch_amenities(response.css(".es-features-list-wrap ul li::text").getall())


        item = ListingLoader(response=response)        
        address_attr = []
        try:
            address_attr  = extract_location_from_coordinates(longitude, latitude)
        except :
            pass
        if address_attr:
            zipcode, city, address = address_attr
            item.add_value("city"                   ,city)
            item.add_value("zipcode"                ,zipcode)



        if not address:
            address = response.css(".es-description p::text").re(".*St.*")
            if address:
                address, city = response.css(".es-description p::text").re(".*St.*")[0].split(".")
                item.add_value("city"                   ,city.strip())
    
        if not landlord_email:
            landlord_email = "pmgr@ranchogroup.com"
        else:
            landlord_email = landlord_email.replace("mailto:","")
        if not landlord_phone:
            landlord_phone = "(604) 684-4508"

        if room_count:
            room_count      = int(room_count.strip())
        
        if available_date:
            available_date = available_date.replace(".","-").strip()

        studio = response.css(".es-description p::text").re("[Ss]tudio")
        house  = response.css(".es-description p::text").re("[Hh]ouse")
        if studio:
            property_type = 'studio'
        elif house:
            property_type = 'house'
        else:
            property_type = 'apartment'
        if not room_count :
            room_count = 1

        item.add_value("external_link"          ,response.url)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_id"            ,external_id)
        item.add_value("position"               ,self.position) # Int
        item.add_value("title"                  ,title)
        item.add_value("address"                ,address)
        item.add_value("latitude"               ,latitude)
        item.add_value("longitude"              ,longitude)
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
        item.add_value("floor"                  ,floor)
        item.add_value("elevator"               ,elevator)
        item.add_value("rent"                   ,rent)
        item.add_value("dishwasher"             ,diswasher)
        item.add_value("washing_machine"        ,washing_machine)
        item.add_value("swimming_pool"          ,swimming_pool)
        item.add_value("landlord_phone"         ,landlord_phone)
        item.add_value("landlord_name"          ,landlord_name)
        item.add_value("landlord_email"         ,landlord_email)
        self.position += 1
        yield item.load_item()



def fetch_amenities(l):
    balcony,diswasher,washing_machine, elevator, parking, pets_allowed, swimming_pool = '','','','','','',''
    for i in l:
        if i:
            i = i.lower()
            if 'balcony' in i:
                balcony = True
    
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
    return balcony,diswasher,washing_machine, elevator, parking, pets_allowed, swimming_pool

   
        


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
    return val
