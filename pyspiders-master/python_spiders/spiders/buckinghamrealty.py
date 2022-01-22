import scrapy
import re
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters

class BuckinghamrealtySpider(scrapy.Spider):
    name = 'buckinghamrealty'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['buckinghamrealty.ca']
    start_urls = ['https://www.buckinghamrealty.ca/residential/properties']
    base_val = 0

    def parse(self, response):
        for ad in response.css(".listing"):
            if int(ad.css(".info-value.price::text").get().split(".")[0].replace("$","").replace(",","")) < 15000:
                yield scrapy.Request(url=ad.css("a.listing_button::attr(href)").get(), callback=self.parse_page )
        
        self.base_val += 24
        if self.base_val <= 554:
            next_page = "https://www.buckinghamrealty.ca/windsor/properties/page-{}".format(str(self.base_val))
            yield scrapy.Request(url=next_page, callback=self.parse)
        
    
    
    def parse_page(self, response):

        if response.css('meta[property="funnl-search:property:status"]::attr(content)').get().lower() == 'for lease':
            external_id         = response.css('meta[property="funnl-search:property:listing:mls"]::attr(content)').get()
            address             = response.css('meta[property="funnl-search:property:location:street"]::attr(content)').get()
            city                = response.css('meta[property="funnl-search:property:location:city"]::attr(content)').get()
            zipcode             = response.css('meta[property="funnl-search:property:location:postal_code"]::attr(content)').get()
            longitude           = response.css('meta[property="funnl-search:property:location:longitude"]::attr(content)').get()
            latitude            = response.css('meta[property="funnl-search:property:location:latitude"]::attr(content)').get()
            landlord_name       = response.css('meta[property="funnl-search:property:agent:name"]::attr(content)').get()
            landlord_phone      = response.css('meta[property="funnl-search:property:agent:phone"]::attr(content)').get()
            title               = response.css('meta[property="og:title"]::attr(content)').get()
            external_link       = response.css('meta[property="og:url"]::attr(content)').get()
            rent                = response.css('.listing_specs .row').re(r'title="Lease: \$[0-9]+,*[0-9]*\.*[0-9]')[0]
            parking             = response.css('.listing_specs .row').re(r'title="Parking:\W*[a-zA-Z0-9]+"')
            property_type       = response.css('.listing_specs .row').re(r'title="Type:\W*[a-zA-Z0-9]+"')
            description         = remove_white_spaces(re.sub("CALL .*","",response.css('.listing_desc::text').get()))
            images              = response.css('.img_gal a.thumbnail::attr(href)').getall()
            square_meters       = response.css('.listing_specs .row').re(r'title="Land Size:\W*[a-zA-Z0-9]+"')
            
            if latitude == "0.0000000" or longitude == "0.0000000":
                latitude = ''
                longitude = ''
            if len(square_meters) > 0:
                try:
                    square_meters = eval(square_meters[0].split(":")[-1].strip().replace('"',"").replace("X","*"))
                except:
                    square_meters = ''
            
            balcony, washing_machine = '', ''
            if 'balcony' in description:
                balcony = True
            if 'laundry' in description:
                washing_machine = True
            
            if len(property_type) > 0:
                property_type = property_type[0].split(":")[-1].strip().replace('"',"")
            if len(parking) > 0:
                parking = parking[0].split(":")[-1].strip().replace('"',"")
            
            
            rent = int(float(rent[rent.find("$")+1:].replace(",","")))
            rooms = response.css('.listing_specs .row').re(r'title="Bed\W*/\W* Bath:.*"')
            room_count,bathroom_count = '',''
            if len(rooms)>0:
                room_count,bathroom_count = re.findall("[0-9]+",rooms[0])[0], re.findall("[0-9]+",rooms[0])[1]
            
                room_count,bathroom_count = int(room_count),int(bathroom_count)
            else:
                room_count = response.xpath('//span[contains(text(), "Bed")]/following-sibling::span/text()').get()
                if room_count:
                    room_count = int(room_count)
            
            item = ListingLoader(response=response)
            item.add_value("external_source"        ,self.external_source)
            item.add_value("external_link"          ,external_link)
            item.add_value("external_id"            ,external_id)
            item.add_value("title"                  ,title)
            item.add_value("address"                ,address)
            item.add_value("rent"                   ,rent)
            item.add_value("images"                 ,images)
            item.add_value("property_type"          ,property_type)
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            item.add_value("square_meters"          ,square_meters)
            item.add_value("parking"                ,parking != "")
            item.add_value("description"            ,description)
            item.add_value("currency"               ,"CAD")
            item.add_value("city"                   ,city)
            item.add_value("landlord_name"          ,landlord_name)
            item.add_value("landlord_phone"         ,landlord_phone)
            item.add_value("landlord_email"         ,'sold@buckinghamrealty.ca')
            item.add_value("zipcode"                ,zipcode)
            item.add_value("longitude"              ,longitude)
            item.add_value("latitude"               ,latitude)
            item.add_value("washing_machine"        ,washing_machine)
            item.add_value("balcony"                ,balcony)       

            if room_count:
                yield item.load_item()
