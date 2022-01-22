import scrapy
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
from python_spiders.loaders import ListingLoader
import requests

class HouseflorenceSpider(scrapy.Spider):
    name = 'houseflorence'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['houseflorence.com']
    start_urls = ['https://www.houseflorence.com/elenco_immobili_f.asp?idm=7448&idcau2=1']

    def parse(self, response):
        for ad in response.css("div.aa-properties-content-body div.col-md-3"):
            if 'Ristorante' in  ad.css("div.aa-properties-about h3 a::text").get() or 'Negozio' in ad.css("div.aa-properties-about h3 a::text").get() or 'ufficio' in  ad.css("div.aa-properties-about h3 a::text").get().lower():
                pass
            else:
                yield scrapy.Request(url=response.urljoin(ad.css("article.aa-properties-item.lista_immobili a.aa-properties-item-img::attr(href)").get()), callback=self.parse_page)

        next_page = response.css("a[aria-label='Next']::attr(href)").get()
        if next_page:
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse)
    def parse_page(self, response):
        falses = ['0','No','NO', 'nessuno']
        item = ListingLoader(response=response)


        title = remove_white_spaces(response.css(".title-properties h2::text").get())
        property_type = make_property_type(remove_white_spaces(response.css(".title-properties h2::text").get()).split(" ")[0])
        address = remove_white_spaces(response.css(".title-properties h2::text").get()).split("Affitto")[1].strip()
        external_id = response.css("ul.property-tags li span::text").get().replace("Rif:","").strip()
        square_meters = int(response.css("ul.property-tags li ::text").re("[Mm][Qq].\W*[A-Za-z0-9]+")[0].replace("Mq.","").strip())
        bathroom_count = response.css("ul.property-tags li ::text").re("[0-9]+\W*[B]agni")
        room_count = response.css("ul.property-tags li ::text").re("[0-9]+\W*[Ll]ocali")
        images = response.css("li a.overlay::attr(href)").getall()
        energy_label = response.xpath('//li[contains(text(), "Classe Energetica:")]/span/text()').get()
        description = remove_white_spaces(response.xpath('//h4[contains(text(), "Descrizione")]/following-sibling::p/text()').get())
        balcony = response.xpath('//div/ul/li[contains(text(), "Balcone")]/text()').get().replace("Balcone:","").strip() not in falses
        terrace = response.xpath('//div/ul/li[contains(text(), "Terrazzo")]/text()').get().replace("Terrazzo:","").strip() not in falses
        floor = response.xpath('//div/ul/li[contains(text(), "Piano")]/text()').get().replace("Piano:","").strip()
        elevator = response.xpath('//div/ul/li[contains(text(), "Ascensore")]/text()').get().replace("Ascensore:","").strip() not in falses
        parking = response.xpath('//div/ul/li[contains(text(), "Box Auto")]/text()').get().replace("Box Auto:","").strip() not in falses
        longitude = response.css("script").re("('[0-9]+\.[0-9]+',\W*'[0-9]+.[0-9]+)'")
        rent = int(response.css("ul.property-tags li ::text").re("[0-9]+\.*[0-9]*\W*€")[0].replace("€","").replace(".","").strip())
        utilities = response.xpath('//div/ul/li[contains(text(), "Spese")]/text()').get()
        if utilities:
            utilities = int(utilities.split(":")[1].strip())
        zipcode, city = '', ''
        if longitude:

            latitude,longitude = longitude[0].replace("'","").split(",")
            item.add_value("latitude"               ,latitude)
            item.add_value("longitude"              ,longitude)

            responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")

            responseGeocodeData = responseGeocode.json()
            
            zipcode = responseGeocodeData['address']['Postal']
            
            city = responseGeocodeData['address']['City']


        if not square_meters :
            square_meters = re.findall("[0-9]+",response.xpath('//div/ul/li[contains(text(), "Superficie:")]/text()').get())[0]
        if room_count:
            room_count = int(room_count[0].replace("locali","").strip())
        if property_type == 'studio':
            room_count = 1
        if bathroom_count:
            bathroom_count = int(bathroom_count[0].replace("Bagni",""))

        if 'Firenze' in title:
            city = 'Firenze'


        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_id"            ,external_id)
        item.add_value("address"                ,address)
        item.add_value("external_link"          ,response.url)
        item.add_value("currency"               ,"EUR")
        item.add_value("images"                 ,images)
        item.add_value("title"                  ,title)
        item.add_value("rent"                   ,rent)
        item.add_value("square_meters"          ,square_meters)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("property_type"          ,property_type)
        item.add_value("parking"                ,parking)
        item.add_value("description"            ,description)
        item.add_value("landlord_phone"         ,'06798680481')
        item.add_value("landlord_name"          ,'HOUSE SRL')
        item.add_value("energy_label"           ,energy_label)
        item.add_value("balcony"                ,balcony)
        item.add_value("floor"                  ,floor)
        item.add_value("elevator"               ,elevator)
        item.add_value("zipcode"                ,zipcode)
        item.add_value("city"                   ,city)
        item.add_value("utilities"              ,utilities)


        yield item.load_item()

def make_property_type(word):
    apartments = ['porzione bifamiliare', 'appartamento', 'loft']
    houses = ['terratetto','villa','indipendente', 'semindipendente']
    studios = ['monolocale']

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
            

