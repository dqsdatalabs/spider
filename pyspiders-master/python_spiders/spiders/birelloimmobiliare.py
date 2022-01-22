import scrapy
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
from python_spiders.loaders import ListingLoader


class BirelloimmobiliareSpider(scrapy.Spider):
    name = 'birelloimmobiliare'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['birelloimmobiliare.com']

    def start_requests(self):
        base_url = 'http://birelloimmobiliare.com/it-immobili-affitto.php?p='

        for i in range(1,8,1):
            yield scrapy.Request(url=base_url+str(i), callback=self.parse)


    def parse(self, response):
        for url in response.css(".ctaImmobiliCont a::attr(href)").getall():
            yield scrapy.Request(url=response.urljoin(url), callback=self.parse_page)


    def parse_page(self, response):
        falses = ['0','No']


        # external_id = str(response.url).split("id=")[1]
        external_id = response.xpath('//td/p/strong[contains(text(), "Riferimento")]/../../../td/p/text()').getall()[0].strip()
        title = response.css(".hgroup h1::text").get()
        description = remove_white_spaces(response.xpath('//h4[contains(text(), "DESCRIZIONE")]/following-sibling::p').css("::text").get())
        images = [response.urljoin(image) for image in response.css(".imgArea.imgAreaDettaglio a::attr(href)").getall()]
        address = response.xpath('//td/p/strong[contains(text(), "Zona")]/../../../td/p/text()').getall()[0].strip()
        square_meters = int(response.xpath('//td/p/strong[contains(text(), "Superficie mq.")]/../../../td/p/text()').getall()[0].strip())
        room_count = int(response.xpath('//td/p/strong[contains(text(), "Numero Locali")]/../../../td/p/text()').getall()[0].strip())
        bathroom_count = int(response.xpath('//td/p/strong[contains(text(), "Numero Bagni")]/../../../td/p/text()').getall()[0].strip())
        parking = "".join(response.xpath('//td/p/strong[contains(text(), "Posto")]/../../../td/p/text()').getall()).strip()
        terrace = response.xpath('//td/p/strong[contains(text(), "Terrazzi")]/../../../td/p/text()').getall()[1].strip() not in falses
        balcony = response.xpath('//td/p/strong[contains(text(), "Balconi")]/../../../td/p/text()').getall()[1].strip() not in falses
        elevator = response.xpath('//td/p/strong[contains(text(), "Ascensore")]/../../../td/p/text()').getall()[1].strip() not in falses
        floor = response.xpath('//td/p/strong[contains(text(), "Piano")]/../../../td/p/text()').getall()[1].strip()
        energy_label = response.xpath('//td/p/strong[contains(text(), "Classe Energetica")]/../../../td/p/text()').getall()[1].strip().replace("Classe","").strip()
        furnished = response.xpath('//td/p/strong[contains(text(), "Arredamento")]/../../../td/p/text()').getall()
        property_type = make_property_type(response.xpath('//td/p/strong[contains(text(), "Tipologia")]/../../../td/p/text()').getall()[0].strip())




        if len(furnished) > 1:
            furnished = furnished[1].strip() not in falses
        else:
            furnished = ''
        


        if parking:
            if 'Classe' in "".join(response.xpath('//td/p/strong[contains(text(), "Posto")]/../../../td/p/text()').getall()).strip():
        
                parking = ''
        rent = response.xpath('//td/p/strong[contains(text(), "Prezzo")]/../../../td/p/text()').getall()[0]
        
        if rent.strip().lower() != "trattativa riservata":
            rent  = int(rent.replace("€","").replace(".","").strip())

            item = ListingLoader(response=response)
            item.add_value("external_source"        ,self.external_source)
            item.add_value("external_link"          ,response.url)
            item.add_value("external_id"            ,external_id)
            item.add_value("title"                  ,title)
            item.add_value("address"                ,address)
            item.add_value("rent"                   ,rent)
            item.add_value("images"                 ,images)
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            item.add_value("square_meters"          ,square_meters)
            item.add_value("parking"                ,parking)
            item.add_value("description"            ,description)
            item.add_value("currency"               ,"EUR")
            item.add_value("terrace"                ,terrace)
            item.add_value("balcony"                ,balcony)
            item.add_value("elevator"               ,elevator)
            item.add_value("floor"                  ,floor)
            item.add_value("energy_label"           ,energy_label)
            item.add_value("furnished"              ,furnished)
            item.add_value("property_type"          ,property_type)
            item.add_value("landlord_name"          ,'Birello Immobiliare S.N.C.')
            item.add_value("landlord_phone"         ,'055/2340329')
            item.add_value("landlord_email"         ,'info@birelloimmobiliare.com')
            item.add_value("city"                   ,'Firenze')
            yield item.load_item()


def make_property_type(word):
    apartments = ['porzione bifamiliare', 'appartamento', 'loft', 'attico']
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
            
