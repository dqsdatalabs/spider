import scrapy
from python_spiders.helper import remove_white_spaces
from python_spiders.loaders import ListingLoader
import re
class HochmannImmobilienSpider(scrapy.Spider):
    name = 'hochmann_immobilien'
    execution_type = 'testing'
    country = 'germany'
    locale = 'de'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['hochmann-immobilien.de', 'hochmann-immobilien.de']
    position = 1

    def start_requests(self):
        start_urls = [            
            {'url': 'https://www.hochmann-immobilien.de/angebote/haeuser-mieten.html',
                'property_type': 'house'},
            {'url': 'https://www.hochmann-immobilien.de/angebote/wohnungen-mieten.html',
                'property_type': 'apartment'},
            ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'), callback=self.parse, meta={'property_type': url.get('property_type')})
            

    def parse(self, response):
        for ad in response.css(".objekteliste a::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(ad), callback=self.parse_page, meta={'property_type':response.meta.get('property_type')})
    
    def parse_page(self, response):
        property_type   = response.meta['property_type']
        square_meters   = round(float(response.xpath('//div[contains(text(), "Wohnfläche")]/following-sibling::div/text()').get().replace("m²","").replace(",",".").strip()))
        room_count      = response.xpath('//div[contains(text(), "Zimmer")]/following-sibling::div/text()').get()
        deposit         = int(response.xpath('//div[contains(text(), "Kaution")]/following-sibling::div/text()').re("[0-9]+\.*[0-9]*")[0].replace(".",""))
        parking         = response.xpath('//div[contains(text(), "Stellplatz")]/following-sibling::div/text()').get()
        rent            = int(response.xpath('//div[contains(text(), "Miete")]/following-sibling::div/text()').re("[0-9]+\.*[0-9]*")[0].replace(".",""))
        title           = response.css(".daten-box-head-right::text").get()
        external_id     = response.css(".daten-box-head-left::text").get()
        address         = response.xpath('//div[contains(text(), "Lage")]/following-sibling::div/text()').get()
        description     = remove_white_spaces(" ".join(response.xpath('//div[contains(text(), "Objektbeschreibung:")]/following-sibling::div/text()').getall()))
        energy_label    = response.css(".objektdetail-box::text").re("Energieeffizienzklasse:\W*[a-zA-Z]")
        images          = [response.urljoin(i) for i in  response.css("#bilder a::attr(href)").getall()]
        landlord_email  = "info@hochmann-immobilien.de"
        landlord_phone  = "0751 36 66 2-20"
        balcony,dishwasher,washing_machine, parking, elevator, terrace = fetch_amenities(response.xpath('//div[contains(text(), "Ausstattung:")]/following-sibling::div/text()').getall())

        item = ListingLoader(response=response)

        if ',' in address:
            address = address.split(",")
            city = re.findall("[a-zA-Z]+", address[1])[0]
            address = address[0]
            item.add_value("city"         ,city)
        
        if energy_label:
            energy_label = energy_label[0].split(":")[1].strip()
        item.add_value("external_source"        ,self.external_source)
        item.add_value("position"               ,self.position)
        item.add_value("external_id"            ,external_id)
        item.add_value("terrace"                ,terrace)
        # item.add_value("pets_allowed"           ,pets_allowed)
        item.add_value("address"                ,address)
        # item.add_value("available_date"         ,available_date)
        item.add_value("external_link"          ,response.url)
        item.add_value("currency"               ,"EUR")
        item.add_value("images"                 ,images)
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("square_meters"          ,square_meters)
        item.add_value("room_count"             ,room_count)
        # item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("property_type"          ,property_type)
        item.add_value("parking"                ,parking)
        item.add_value("description"            ,description)
        item.add_value("landlord_phone"         ,landlord_phone)
        item.add_value("landlord_name"          ,'Immobilienbüro Hochmann')
        item.add_value("landlord_email"         ,landlord_email)
        item.add_value("energy_label"           ,energy_label)
        item.add_value("balcony"                ,balcony)
        item.add_value("dishwasher"             ,dishwasher)
        item.add_value("elevator"               ,elevator)
        item.add_value("deposit"                ,deposit)
        item.add_value("washing_machine"        ,washing_machine)
        self.position += 1
        yield item.load_item()



def fetch_amenities(l):
    balcony,diswasher,washing_machine, parking, elevator, terrace = '','','','','',''
    for i in l:
        if 'balkon' in i.lower():
            balcony = True
        elif 'personenaufzug' in i.lower():
            elevator = True
        elif 'dishwasher' in i.lower():
            diswasher = True
        elif 'waschmaschine' in i.lower() or 'wasch' in i.lower():
            washing_machine = True
        elif 'tierhaltung NICHT erlaubt' in i.lower():
            pets_allowed = False
        elif 'parkhaus in fußnähe' in i.lower() or 'außenstellplatz' in i.lower():
            parking = True
        elif 'terrasse' in i.lower():
            terrace = True
    return balcony,diswasher,washing_machine, parking, elevator, terrace