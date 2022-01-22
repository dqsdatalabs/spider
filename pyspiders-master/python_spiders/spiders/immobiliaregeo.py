import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces

class ImmobiliaregeoSpider(scrapy.Spider):
    name = 'immobiliaregeo'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['immobiliaregeo.it']
    start_urls = ['https://www.immobiliaregeo.it/agenzia-immobiliare-pietrasanta-affitto-immobili']

    def parse(self, response):
        for url in response.css(".property-action a::attr(href)").getall():
            yield scrapy.Request(url=url, callback=self.parse_page)
        next_page = response.css("ul.pagination li a[rel='next']::attr(href)").get()
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse)

    def parse_page(self, response):
        apartments = ['porzione bifamiliare', 'appartamento', 'loft','semindipendente']
        houses = ['terratetto','villa','indipendente']
        rent = int(response.xpath('//span[contains(text(), "Richiesta")]/following-sibling::span/text()').get().replace("€",""))
        
        if rent != 0:
            external_id = response.css("h1.property-title::text").getall()[0].replace("RIF:","").strip()
            address = response.xpath('//span[contains(text(), "Localita")]/following-sibling::span/a/text()').get()
            description = remove_white_spaces(response.css(".property-desc p::text").get())
            title = response.xpath('//h1/following-sibling::text()').get().strip()
            images = response.css("ul li a img::attr(src)").getall()
            property_type = response.xpath('//span[contains(text(), "Tipo")]/following-sibling::span/a/text()').get().lower()
            square_meters = int(response.xpath('//span[contains(text(), "Mq")]/following-sibling::span/text()').get().replace("mq","").strip())
            room_count = int(response.xpath('//span[contains(text(), "Camere")]/following-sibling::span/text()').get().strip())
            bathroom_count = int(response.xpath('//span[contains(text(), "Bagni")]/following-sibling::span/text()').get().strip())
    
            furnished, terrace, parking  = fetch_amenities(response.css(".detail-field span.detail-field-value::text").getall())

            landlord_phone = response.xpath('//i[contains(@class, "fa-phone")]/following-sibling::text()').get()
            landlord_email = response.xpath('//i[contains(@class, "fa-envelope-square")]/following-sibling::text()').get()
            city = response.css(".property-title small::text").get().split("-")[0].strip()
            for apart in apartments:
                if apart in property_type:
                    property_type = 'apartment'
                    break
            
            for house in houses:
                if  house in  property_type :
                    property_type = 'house'
                    break


            item = ListingLoader(response=response)
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
            item.add_value("landlord_phone"         ,landlord_phone)
            item.add_value("landlord_email"         ,landlord_email)
            item.add_value("furnished"              ,furnished)
            item.add_value("terrace"                ,terrace)
            item.add_value("city"                   ,city) 
            item.add_value("landlord_name"          ,'Geo Immobiliare')
            yield item.load_item()

def fetch_amenities(li):
    furnished, terrace, parking = '', '',''
    for i in li:
        if i.lower() == 'terrazzo':
            terrace = True
        elif i.lower() == 'arredato':
            furnished = True
        elif i.lower() == 'posto auto':
            parking = True
    return furnished, terrace, parking
