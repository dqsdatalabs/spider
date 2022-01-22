import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import remove_white_spaces,extract_location_from_address, extract_location_from_coordinates
import re

class BoyeImmobilienSpider(scrapy.Spider):
    name = 'boye_immobilien'
    execution_type = 'testing'
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['boye-immobilien.de']
    start_urls = ['https://boye-immobilien.de/immobilienangebote/immobilie-mieten']
    position = 1


    def parse(self, response):
        for url in response.css("h2 a"):
            if "Ladenlokal" not in  url.css("a::text").get():
                yield scrapy.Request(url=url.css("a::attr(href)").get(), callback=self.parse_page)


    def parse_page(self, response):
        title           = response.css("#openestate_expose_header h2::text").get()
        external_id     = response.xpath('//div[contains(text(), "Objekt-Nr")]/following-sibling::text()').get()
        property_type   = "apartment"
        address         = response.xpath('//div[contains(text(), "Adresse")]/following-sibling::text()').get()
        rent            = int(response.xpath('//div[contains(@id,"openestate_expose_view_content" )]/ul/li[contains(text(), "Kaltmiete:")]/b/text()').get().split(",")[0].replace(".",""))
        utilities       = int(response.xpath('//div[contains(@id,"openestate_expose_view_content" )]/ul/li[contains(text(), "Nebenkosten:")]/b/text()').get().split(",")[0].replace(".",""))
        deposit         = response.xpath('//div[contains(@id,"openestate_expose_view_content" )]/ul/li[contains(text(), "Kaution")]/b/text()').get()
        square_meters   = response.xpath('//li[contains(text(), "Wohnfläche")]/b/text()').get()
        room_count      = int(response.xpath('//li[contains(text(), "Zimmer")]/b/text()').get().split(",")[0].strip())
        bathroom_count  = response.xpath('//li[contains(text(), "WC")]/b/text()').get()
        parking         = response.xpath('//li[contains(text(), "Stellplätze")]/b/text()').get()
        balcony         = response.xpath('//li[contains(text(), "Balkon")]/b/text()').get()
        floor           = response.xpath('//li[contains(text(), "Etage")]/b/text()').get()
        terrace         = response.xpath('//li[contains(text(), "Terrasse")]/b/text()').get()
        if balcony:
            balcony = True
        if terrace:
            terrace = True
        if parking:
            parking = True
        if bathroom_count:
            bathroom_count  = int(bathroom_count.split(",")[0].strip())
        if room_count == 0:
            room_count = 1
            property_type = 'studio'
        if square_meters:
            square_meters = int(square_meters.replace("m²","").replace("ca.","").split(",")[0].strip())
        if deposit:
            deposit     = int(deposit.split(",")[0].replace(".",""))
        longitude, latitude     = extract_location_from_address(address)
        zipcode, city, address  = extract_location_from_coordinates(longitude, latitude)
        item = ListingLoader(response=response)
        item.add_value("external_link"          ,response.url)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_id"            ,external_id)
        item.add_value("position"               , self.position) # Int
        item.add_value("title"                  ,title)
        item.add_value("city"                   ,city)
        item.add_value("zipcode"                ,zipcode)
        item.add_value("address"                ,address)
        item.add_value("latitude"               ,str(latitude))
        item.add_value("longitude"              ,str(longitude))
        item.add_value("property_type"          ,property_type)
        item.add_value("square_meters"          ,square_meters)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("terrace"                ,terrace)
        item.add_value("currency"               ,"EUR")
        item.add_value("parking"                ,parking)
        item.add_value("balcony"                ,balcony)
        item.add_value("floor"                  ,floor)
        item.add_value("rent"                   ,rent)
        item.add_value("deposit"                ,deposit)
        item.add_value("utilities"              ,utilities)        
        self.position += 1
        yield scrapy.Request(url=response.xpath('//a[contains(text(), "Galerie")]/@href').get(), callback=self.fetch_images, meta={"item":item, "id":external_id})



    def fetch_images(self, response):
        item = response.meta['item']
        ex_id = response.meta['id']
        images       = ["https://boye-immobilien.de/wp-content/uploads/immoexport/data/"+ex_id+"/"+i.replace("&x=100&y=75","").split("img=")[1] for i in response.css("#openestate_expose_gallery a img::attr(src)").getall()]
        item.add_value("images"                 ,images)
        yield scrapy.Request(url=response.xpath('//a[contains(text(), "Beschreibung")]/@href').get(), callback=self.fetch_description, meta={"item":item})

    def fetch_description(self, response):
        item = response.meta['item']
        description     = response.xpath('//h3[contains(text(), "Kurzbeschreibung")]/following-sibling::p').css(" ::text").get()
        item.add_value("description"            ,re.sub("Tel.*","",description))
        yield scrapy.Request(url=response.xpath('//a[contains(text(), "Kontakt")]/@href').get(), callback=self.fetch_landlord_data, meta={"item":item})

    def fetch_landlord_data(self, response):
        
        item = response.meta['item']

        landlord_name       = response.xpath('//li/div[contains(text(), "Name:")]/following-sibling::text()').get()
        landlord_phone      = response.xpath('//li/div[contains(text(), "Telefon:")]/following-sibling::text()').get()
        
        item.add_value("landlord_phone"         ,landlord_phone)
        item.add_value("landlord_name"          ,landlord_name)
        item.add_value("landlord_email"         ,'info(at)boye-immobilien.de')
        yield item.load_item()




