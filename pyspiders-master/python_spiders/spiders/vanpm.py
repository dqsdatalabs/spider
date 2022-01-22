import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import MapCompose, TakeFirst
from w3lib.html import remove_tags
from scrapy.item import Item, Field
from scrapy.loader.processors import TakeFirst
from python_spiders.loaders import ListingLoader
class VanpmSpider(scrapy.Spider):
    name = 'Vanpm_PySpider_Canada'
    allowed_domains = ['vanpm.com']
    start_urls = ['https://vanpm.com/']

    def parse(self, response):
        for url in response.css(".property-thumb a::attr(href)").extract():
            yield scrapy.Request(url, callback=self.parse_page)
    

    def parse_page(self, response):
        




            images          = response.css("ul.slides li img::attr(src)").extract()
            

            address         = response.css(".property-section h1::text").get()
            rent            = response.css(".property-section .price::text").get()
            square_meters   = response.css('div.col-1-1.property-section ul li').extract()
            room_count      = response.css('div.col-1-1.property-section ul li').extract()

            bathroom_count  = response.css('div.col-1-1.property-section ul li').extract()
            title           = response.css('.property-section h1::text').get()


            description     = response.xpath('//h3[contains(text(), "PROPERTY DESCRIPTION")]/following-sibling::p/text()').get()
            available_date  = response.xpath('//h3[contains(text(), "AVAILABILITY:")]/following-sibling::p/text()').get()
            deposit         = response.xpath('//div[contains(@class, entry-content)]/h3[contains(text(), "TERMS:")]/following-sibling::p/text()[2]').get()
            utilities       = response.xpath('//div[contains(@class, entry-content)]/h3[contains(text(), "TERMS:")]/following-sibling::p[contains(text(),UTILITIES)]').get()
            
            currency        = "CAD"
            landlord_phone  = '6047227723'
            external_link   = response.url
            landlord_email  = "info@vanpm.com"
            landlord_name   = "Patty Ho"
            external_source = "Vanpm_PySpider_Canada"
            property_type   = "appartment"
            
            







            room_count      = make_rooms(room_count)
            square_meters   = make_area(square_meters)
            bathroom_count  = get_toilets(bathroom_count)
            rent            = make_int(rent)
            utilities       = make_utilities(utilities)
            deposit         = make_striped(deposit)




            for items in response.css("div.site-content"):
                if  not isinstance(rent, int):
                    continue
                
                default_output_processor = TakeFirst()
                item = ListingLoader(response=response)

            
                item.add_value("images"                   ,images)
                item.add_value("address"                  ,address)
                item.add_value("rent"                     ,rent)
                item.add_value("square_meters"            ,int(int(square_meters)*10.764))
                item.add_value("room_count"               ,room_count)
                item.add_value("bathroom_count"           ,bathroom_count)
                item.add_value("title"                    ,title)


                item.add_value("description"            ,description)
                item.add_value("available_date"         ,available_date)
                item.add_value("deposit"                ,deposit)
                item.add_value("utilities"              ,utilities)

                item.add_value("currency"               ,currency)
                item.add_value("landlord_phone"         ,landlord_phone)
                item.add_value("external_link"          ,external_link)
                item.add_value("landlord_email"         ,landlord_email)
                item.add_value("landlord_name"          ,landlord_name)
                item.add_value("external_source"        ,external_source)


                item.add_value("property_type"          ,property_type)

                yield item.load_item()



def make_rooms(val):
    for l in val:
        i = l.find("Beds: ")
        

        if i > -1:
            i = int(l[i+6:i+7])

            return i
    return ''

def make_area(val):
    for l in val:
        
        i = l.find("Area: ")
        if i <= -1:
            continue
        c = ''
        i = i+6
        for n in range(i, len(l),1):
            # 
            if l[n] in '1234567890':
                c+=l[n]
            else:
                try:
                    c = int(c)
                except:
                    pass
                return c
    return ''
    

def get_toilets(val):
    for l in val:
        i = l.find("Baths:")
        if i > -1:
            i = int(l[i+7:i+8])
            return i
    return ''

def make_int(val):
    try:
        return int(val)
    except:
        return val

def make_utilities(val):
    try:
        val = remove_tags(val).split("\n")
        for i in val:
            if "Utilities" in i :
                return i
    except:
        return ''
    

def make_striped(val):
    try:
        return val.strip()
    except:
        return ''
