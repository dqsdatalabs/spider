import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, extract_location_from_coordinates
import re
class WredevelopmentSpider(scrapy.Spider):
    name = 'wredevelopment'
    execution_type = 'testing'
    country = 'canada'
    locale = 'ca'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['wredevelopment.ca']
    start_urls = ['https://wredevelopment.ca/properties/?realteo_order=&keyword_search=']
    position = 1
    def parse(self, response):
        for url in response.css(".listing-title h4 a::attr(href)").getall():
            yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
        
        title           = response.css(".property-title h2::text").get()
        property_type   = make_property_type(response.css(".property-type-badge::text").get())
        # address         = "".join(response.css(".property-title .listing-address::text").getall()).strip()
        rent            = response.css(".property-pricing div::text").get()
        images          = response.css(".property-slider a::attr(href)").getall()
        room_count      = response.css("li.main-detail-_bedrooms span::text").get()
        bathroom_count  = response.css("li.main-detail-_bathrooms span::text").get()
        description     = remove_white_spaces(" ".join(response.css(".property-description p::text").getall()))
        latitude        = response.css("#propertyMap::attr(data-latitude)").get()
        longitude       = response.css("#propertyMap::attr(data-longitude)").get()

        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        balcony, swimming_pool, washing_machine, diswasher, parking = search_in_desc(description)
        balcony,diswasher,washing_machine, elevator, parking = fetch_amenities(response.css(".property-features li a::text").getall())



        if '-' in bathroom_count:
            bathroom_count = int((int(bathroom_count.split("-")[0]) + int(bathroom_count.split("-")[1]))/2)

        if '-' in room_count:
            room_count = int((int(room_count.split("-")[0]) + int(room_count.split("-")[1]))/2)

        if '-' in rent:
            rent            = int(rent.split("-")[0].replace(",","").replace("$","").strip())
        elif "Call for Pricing" in rent:
            rent = ""
        landlord_phone = response.xpath('//h4[contains(text(), "Phone:")]/a/b/text()').re("[0-9]+-[0-9]+-[0-9]+")
        if landlord_phone:
            landlord_phone = landlord_phone[0]
        else:
            landlord_phone = "(204) 889-5409"
        counter = 1
        for property in response.css("div.accordion h3"):
            temp = property.css("h3::text").get()
            if 'bachelor' in temp.lower():
                property_type = 'studio'
                room_count = 1
            elif 'bedroom' in temp.lower():
                temp_room = re.findall("[0-9]+\W*[Bb]edroom",temp.lower())
                if temp_room:
                    room_count = int(temp_room[0].replace("bedroom","").strip())
            elif 'penthouse' in temp.lower():
                property_type = 'house'
            rent = property.css("span.cmb-type-text-money::text").re("\$[0-9]+\,*[0-9]*")
            if rent:
                rent = int(rent[0].replace("$","").replace(",",""))

                square_meters = int(property.css("span.fpArea::text").re("[0-9]+,*[0-9]*")[0])
                item = ListingLoader(response=response)
                item.add_value("external_link"          ,response.url+"#"+str(counter))
                item.add_value("external_source"        ,self.external_source)
                item.add_value("position"               , self.position) # Int
                item.add_value("title"                  ,title)
                item.add_value("city"                   ,city)
                item.add_value("zipcode"                ,zipcode)
                item.add_value("address"                ,address)
                item.add_value("latitude"               ,latitude)
                item.add_value("longitude"              ,longitude)
                item.add_value("property_type"          ,property_type)
                item.add_value("washing_machine"        ,washing_machine)
                item.add_value("swimming_pool"          ,swimming_pool)
                item.add_value("room_count"             ,room_count)
                item.add_value("bathroom_count"         ,bathroom_count)
                item.add_value("currency"               ,"CAD")
                item.add_value("parking"                ,parking)
                item.add_value("images"                 ,images)
                item.add_value("balcony"                ,balcony)
                item.add_value("elevator"               ,elevator)
                item.add_value("rent"                   ,rent)
                item.add_value("description"            ,description)
                item.add_value("dishwasher"             ,diswasher)
                item.add_value("landlord_phone"         ,landlord_phone)
                counter +=1
                self.position += 1
                yield item.load_item()



def make_property_type(word):
    apartments = ['apartments']
    houses = []
    studios = []

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



def search_in_desc(desc):
    balcony, swimming_pool, washing_machine, diswasher, parking = '', '', '', '',''
    desc = desc.lower()

    if 'laundry' in desc:
        washing_machine = True

    if 'balcon' in desc:
        balcony = True
    if 'pool' in desc:
        swimming_pool = True

    if 'dishwasher' in desc or 'laundry' in desc:
        diswasher = True
    if 'garbage' in desc or 'parking' in desc:
        parking = True


    return balcony, swimming_pool, washing_machine, diswasher, parking




def fetch_amenities(l):
    balcony,diswasher,washing_machine, elevator, parking = '','','','',''
    for i in l:
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
        elif 'pool' in i:
            swimming_pool = True
    return balcony,diswasher,washing_machine, elevator, parking