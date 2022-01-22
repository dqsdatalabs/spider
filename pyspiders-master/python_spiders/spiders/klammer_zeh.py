import scrapy
from ..loaders import ListingLoader
from python_spiders.helper import remove_white_spaces

class KlammerZehSpider(scrapy.Spider):
    name = 'klammer_zeh'
    execution_type = 'testing'
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['klammer-zeh.de']
    start_urls = ['https://klammer-zeh.de/angebote#filter=.page1']
    position = 1

    def parse(self, response):
        for url in  response.css("#estate_list a.estate::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_page)

    def parse_page(self, response):

        rent            = response.xpath('//div[contains(text(), "Kaltmiete")]/following-sibling::div/text()').get()
        title           = response.css(".obj-header h2::text").get().strip() 
        if rent and 'Außenstellplätze' not in title:
            rent         = int(rent.split(",")[0].replace(".",""))

            utilities       = response.xpath('//div[contains(text(), "Nebenkosten")]/following-sibling::div/text()').get()
            available_date  = response.xpath('//div[contains(text(), "verfügbar ab")]/following-sibling::div/text()').get() 
            square_meters   = response.xpath('//div[contains(text(), "Wohnfläche")]/following-sibling::div/text()').get()
            room_count      = response.xpath('//div[contains(text(), "Zimmer")]/following-sibling::div/text()').get()
            bathroom_count  = response.xpath('//div[contains(text(), "Badezimmer")]/following-sibling::div/text()').get()
            elevator        = response.xpath('//div[contains(text(), "Personenfahrstuhl")]/following-sibling::div/text()').get()
            parking         = response.xpath('//div[contains(text(), "Garage")]/following-sibling::div/text()').get() in ["Ja", "ja"]
            description     = remove_white_spaces("".join(response.css(".description::text").getall()))
            energy_label    = response.xpath('//div[contains(text(), "Energieeffizienzklasse")]/following-sibling::div/text()').get()
            landlord_phone  = remove_white_spaces("||".join(response.css(".contact::text").getall())).split("||")[-1].split(":")[1].strip()
            landlord_name   = remove_white_spaces("||".join(response.css(".contact::text").getall())).split("||")[-2].strip()
            landlord_email  = "info@klammer-zeh.de"
            images          = [response.urljoin(i) for i in response.css(".small-pics .pic a::attr(href)").getall()]
    
            if elevator:
                if elevator in ["Ja", "ja"]:
                    elevator = True
                elif elevator in ['Nein', 'nein']:
                    elevator = False
            if room_count:
                room_count = int(room_count)
            if bathroom_count:
                bathroom_count = response.xpath('//div[contains(text(), "Badezimmer")]/following-sibling::div/text()').get()

            if available_date:
                available_date  = "-".join(available_date.split(".")[::-1])
            if utilities:
                utilities    = int(utilities.split(",")[0].replace(".",""))
            if square_meters:
                square_meters = int(square_meters.replace("m²",""))

            item = ListingLoader(response=response)
            item.add_value("external_link"          ,response.url)
            item.add_value("external_source"        ,self.external_source)
            # item.add_value("external_id"            ,external_id)
            item.add_value("position"               , self.position) # Int
            item.add_value("title"                  ,title)
            # item.add_value("city"                   ,city)
            # item.add_value("zipcode"                ,zipcode)
            # item.add_value("address"                ,address)
            # item.add_value("latitude"               ,str(latitude))
            # item.add_value("longitude"              ,str(longitude))
            item.add_value("description"            ,description)
            item.add_value("property_type"          ,"apartment")
            item.add_value("square_meters"          ,square_meters)
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            # item.add_value("terrace"                ,terrace)
            # item.add_value("pets_allowed"           ,pets_allowed)
            item.add_value("available_date"         ,available_date)
            item.add_value("currency"               ,"EUR")
            item.add_value("parking"                ,parking)
            item.add_value("energy_label"           ,energy_label)
            # item.add_value("balcony"                ,balcony)
            # item.add_value("floor"                  ,floor)
            item.add_value("elevator"               ,elevator)
            item.add_value("rent"                   ,rent)
            # item.add_value("deposit"                ,deposit)
            item.add_value("images"                 ,images)
            item.add_value("utilities"              ,utilities)
            item.add_value("landlord_phone"         ,landlord_phone)
            item.add_value("landlord_name"          ,landlord_name)
            item.add_value("landlord_email"         ,landlord_email)
            self.position += 1
            yield item.load_item()
