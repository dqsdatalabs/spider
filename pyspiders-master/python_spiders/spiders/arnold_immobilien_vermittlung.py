import scrapy
import dateutil.parser
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, extract_location_from_address, extract_location_from_coordinates


class ArnoldImmobilienVermittlungSpider(scrapy.Spider):
    name = 'arnold_immobilien_vermittlung'
    execution_type = 'testing'
    country = 'germny'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['arnold-immobilien-vermittlung.de']
    start_urls = ['https://arnold-immobilien-vermittlung.de/immobilien/']
    position = 1

    def parse(self, response):
        for property in response.css(".property"):
            if int(property.css(".col-xs-7.dd.price::text").re("[0-9]+\.*[0-9]*")[0].replace(".","")) < 50000:
                yield scrapy.Request(url=property.css(".property-title a::attr(href)").get(), callback=self.parse_page)

    def parse_page(self, response):
        title           = response.css(".property-title::text").get()
        address         = response.xpath('//span[contains(@class, "glyphicon glyphicon-map-marker")]/following-sibling::text()').get().strip() 
        images          = response.css("#immomakler-galleria a::attr(href)").getall()
        external_id     = response.xpath('//div[contains(text(), "Objekt ID")]/following-sibling::div/text()').get()
        property_type   = make_property_type(response.xpath('//div[contains(text(), "Objekttyp")]/following-sibling::div/text()').get().strip())
        square_meters   = int(response.xpath('//div[contains(text(), "Gesamtfläche")]/following-sibling::div/text()').get().replace("m²","").strip())
        room_count      = int(response.xpath('//div[contains(text(), "Zimmer")]/following-sibling::div/text()').re("[0-9]+")[0].strip())
        available_date  = response.xpath('//div[contains(text(), "Verfügbar ab")]/following-sibling::div/text()').re("[0-9]+\.[0-9]+\.[0-9]+")
        rent            = int(response.xpath('//div[contains(text(), "Kaltmiete")]/following-sibling::div/text()').re("[0-9]+\.*[0-9]*")[0].replace(".","").strip())
        utilities       = int(response.xpath('//div[contains(text(), "Nebenkosten")]/following-sibling::div/text()').re("[0-9]+\.*[0-9]*")[0].replace(".","").strip())
        description     = response.css(".panel-body p::text").get()
        landlord_name   = response.css(".dd span.p-name::text").get()
        landlord_email  = response.css(".u-email a::text").get()
        landlord_phone  = response.css(".p-tel a::text").get()
        longitude, latitude = extract_location_from_address(address)
        zipcode, city, address = extract_location_from_coordinates(longitude, latitude)

        balcony,dishwasher,washing_machine,parking = fetch_amenities(response.css("ul .list-group-item::text").getall())
        
        if available_date:
            available_date = dateutil.parser.parse(available_date[0]).strftime("%Y-%m-%d")


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
        item.add_value("utilities"              ,utilities)
        item.add_value("square_meters"          ,square_meters)
        item.add_value("parking"                ,parking)
        item.add_value("description"            ,description)
        item.add_value("currency"               ,"EUR")
        item.add_value("city"                   ,city)
        item.add_value("landlord_name"          ,landlord_name)
        item.add_value("landlord_phone"         ,landlord_phone)
        item.add_value("landlord_email"         ,landlord_email)
        item.add_value("zipcode"                ,zipcode)
        item.add_value("longitude"              ,str(longitude))
        item.add_value("latitude"               ,str(latitude))
        item.add_value("washing_machine"        ,washing_machine)
        item.add_value("balcony"                ,balcony)       
        item.add_value("available_date"         ,available_date)       
        item.add_value("dishwasher"             ,dishwasher)
        item.add_value("position"               ,self.position)
        
        self.position += 1
        if 'büro/praxis' not in property_type.lower():
            yield item.load_item()



def make_property_type(word):
    apartments = ['wohnung']
    houses = []
    studios = []

    for apart in apartments:
        if apart in word.lower():
            return'apartment'
                  
    # for house in houses:
    #     if  house in  word.lower() :
    #         return 'house'
    # for studio in studios:
    #     if  studio in  word.lower() :
    #         return 'studio'
    return word


def fetch_amenities(l):
    balcony,dishwasher,washing_machine, parking = '','','',''
    for i in l:
        if 'balkon' in i.lower():
            balcony = True
        elif 'dishwasher' in i.lower():
            dishwasher = True
        elif 'wasch' in i.lower():
            washing_machine = True
        elif 'parkhaus' in i.lower() or 'außenstellplatz' in i.lower() or 'garage' in i.lower():
            parking = True

    return balcony,dishwasher,washing_machine,parking
