import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
import re

class JamiedempsterSpider(scrapy.Spider):
    name = 'jamiedempster'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['jamiedempster.ca']
    start_urls = ['https://www.jamiedempster.ca/property/?view=grid']

    def parse(self, response):
        for page in response.css("div.rh_prop_card__wrap .rh_prop_card__thumbnail a::attr(href)").getall():
            yield scrapy.Request(url=page, callback=self.parse_page)

    
    def parse_page(self, response):
        status =    remove_white_spaces(response.css(".rh_page__property_price p.status::text").get())
        if status == "For Rent":
            title           = remove_white_spaces(response.css("h1.rh_page__title::text").get())
            address         = remove_white_spaces(response.css("h1.rh_page__title::text").get())
            rent            = int(response.css("p.price::text").re("[0-9]+,*[0-9]*")[0].replace(",",""))
            images          = response.css("ul.slides li a.venobox::attr(href)").getall()
            external_id     = remove_white_spaces(response.css("div.rh_property__id p.id::text").get())
            room_count      = int(eval(response.xpath('//div/span[contains(text(), "Bedrooms")]/following-sibling::div/span').css("::text").get()))
            bathroom_count  = int(eval(response.xpath('//div/span[contains(text(), "Bathrooms")]/following-sibling::div/span').css("::text").get()))
            square_meters   = make_square(response.xpath('//div[contains(@class,"rh_property__meta_wrap")]/div/span[contains(text(), "Area")]/following-sibling::div/span').css("::text").get())
            description     = remove_white_spaces(" ".join(response.css("div.rh_content ::text").getall()))
            parking         = response.xpath('//div/span[contains(text(), "Garage")]/following-sibling::div/span').css("::text").get()
            f_parking       = response.xpath('//li/a[contains(text(), "Parking")]/text()').get()
            f_garage        = response.xpath('//li/a[contains(text(), "Garage")]/text()').get()
            washing_machine = response.xpath('//li/a[contains(text(), "Laundry")]/text()').get()
            swimming_pool   = response.xpath('//li/a[contains(text(), "Swimming Pool")]/text()').get()
            utilities       = cal_utils(response.xpath('//li/span[contains(text(), "Taxes:")]/following-sibling::span').css("::text").re("\$[0-9]+,*[0-9]*\.*[0-9]*"))
            landlord_name   = response.css(".rh_property_agent h3.rh_property_agent__title::text").get()
            landlord_phone  = response.css(".rh_property_agent div.rh_property_agent__agent_info .mobile a::text").get()
            landlord_email  = response.css(".rh_property_agent div.rh_property_agent__agent_info .email a::text").get()
            latitude        = re.findall('[0-9]+\.[0-9]+',response.css("script").re('lat":"[0-9]+\.[0-9]+"')[0])[0]
            longitude       = re.findall('-[0-9]+\.[0-9]+',response.css("script").re('lng":"-[0-9]+\.[0-9]+"')[0])[0]
            floor           = response.xpath('//div[contains(@class,"rh_property__meta_wrap")]/div/span[contains(text(), "Lot Size")]/following-sibling::div/span').css("::text").get()
            
            if floor:
                floor = re.findall("[0-9]+nd",floor)
                if floor:
                    floor = floor[0].replace("nd","")
                        
            if parking or f_parking or f_garage:
                parking = True
            if swimming_pool:
                swimming_pool = True
            
            if washing_machine:
                washing_machine = True

            
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
            item.add_value("square_meters"          ,int(int(square_meters)*10.764))
            item.add_value("parking"                ,parking)
            item.add_value("description"            ,description)
            item.add_value("currency"               ,"CAD")
            item.add_value("utilities"              ,utilities)
            item.add_value("landlord_name"          ,landlord_name)
            item.add_value("landlord_phone"         ,landlord_phone)
            item.add_value("landlord_email"         ,landlord_email)
            item.add_value("latitude"               ,latitude)
            item.add_value("longitude"              ,longitude)
            item.add_value("floor"                  ,floor)
            item.add_value("washing_machine"        ,washing_machine)

            yield item.load_item()

            
            
def cal_utils(util):
    if len(util) > 0:
        return round(float(util[0].replace("$","").replace(",","")))

def make_square(sq):
    if sq:
        if '-' in sq:
            min = sq.split("-")[0]
            max = sq.split("-")[-1]
            return round((int(max)+int(min))/2)
        return sq_feet_to_meters(int(remove_white_spaces(sq).replace(",","")))
