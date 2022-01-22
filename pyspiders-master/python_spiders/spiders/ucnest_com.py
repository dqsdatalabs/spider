# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_rent_currency, extract_number_only
import json
import re

class UcnestSpider(scrapy.Spider):
    name = "ucnest_com"
    allowed_domains = ['www.ucnest.com']
    start_urls = ['https://www.ucnest.com']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source = "Ucnest_PySpider_united_kingdom_en"
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.ucnest.com/accommodation/city/London",
                ],
                "city": "London"
            },
	        {
                "url": [
                    "https://www.ucnest.com/accommodation/city/Sheffield"
                ],
                "city": "Sheffield"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
                    for item in url.get('url'):
                        yield scrapy.Request(
                            url=item,
                            callback=self.parse,
                            meta={'city': url.get('city')}
                        )

# 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//h3[contains(@id,'detail_title')]//a/@href").extract():
            follow_url = response.urljoin(item)
            yield scrapy.Request(follow_url, callback=self.get_property_details, meta={'city': response.meta.get('city')})

                    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        address = response.xpath('//p[@class="detail_title_small"]/text()[normalize-space()]').extract_first()
        description = "".join(response.xpath('//div[@class="viewcon_txt"]/p/text()').extract())

        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))

   
        item_loader.add_value("address", re.sub("\s{2,}", " ", address))
        
        rent_string = "".join(response.xpath('.//span[contains(text(), "Entire property")]/../../../..//span[@class="pw"]/text()').extract())
        if rent_string:
            rent=rent_string.split("£")[-1].replace("\r","").replace("\n","").strip()
            item_loader.add_value('rent', int(rent)*4)
        else:
            rent=response.xpath("//span[@class='pw']/text()").get()
            if rent:
                rent=rent.split("£")[-1].replace("\r","").replace("\n","").strip()
                item_loader.add_value("rent",int(rent)*4)
        item_loader.add_value("currency","GBP")
        deposit = response.xpath('.//span[contains(text(), "Entire property")]/../../../..//span[@class="hs_span"]/../text()').extract()
        if len(deposit) > 0:
            item_loader.add_value('deposit', "".join(deposit))

        item_loader.add_value('city', response.meta.get('city'))
        title = response.xpath('//p[@class="detail_title"]/text()').get()
        item_loader.add_value('title', title)

        item_loader.add_xpath('room_count', '//li[@class="woshi"]/span/text()')
        item_loader.add_xpath('bathroom_count', '//li[@class="weiyu"]/span/text()')
        images = [response.urljoin(x.split("?")[0]) for x in response.xpath("//a[@class='pirobox_t6 example']/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)        
        item_loader.add_xpath('floor', '//div[@class="bzcon_sec"]/p/span[2]/text()')
        item_loader.add_value('description', description)
        item_loader.add_value('landlord_phone', '+44 20 7043 1185')
        item_loader.add_value('landlord_name','Urban and Campus Nest Ltd,')
        item_loader.add_value('landlord_email','info@ucnest.com')

        pets = response.xpath('//span[@class="pet"]/../text()').extract_first()
        if "no pets" in pets.lower():
            item_loader.add_value('pets', False)
        
        washing_machine = response.xpath('//span[@id="features-icon-Washingmachine"]').extract_first()
        if washing_machine:
            item_loader.add_value('washing_machine', True)

        utilities = ", ".join(response.xpath('//div[@class="keyfe clearfix"]//li/text()').extract())

        if "parking" in utilities.lower():
            item_loader.add_value('parking', True)
        
        if "terrace" in utilities.lower():
            item_loader.add_value('terrace', True)

        if "swimming pool" in utilities.lower():
            item_loader.add_value('swimming_pool', True)
        
        if "elevator" in utilities.lower() or "lift" in utilities.lower():
            item_loader.add_value('elevator', True)
        
        if "balcony" in utilities.lower():
            item_loader.add_value('balcony', True)

        if "unfurnished" in utilities.lower():
            item_loader.add_value('furnished', False)
        elif "furnished" in utilities.lower():
            item_loader.add_value('furnished', True)
        
        if not item_loader.get_collected_values("bathroom_count"):
            bathroom_count = response.xpath("//img[@title='Shower room']/../span/text()").get()
            if bathroom_count: item_loader.add_value('bathroom_count', bathroom_count.strip())
        yield item_loader.load_item()

        room_listing = response.xpath("//div[@class='aboutroomcon_de']/div[@class='abtroomdetail ']")
        if room_listing:          
            for item in room_listing:
                status = response.xpath("//div[@class='wq_item_img']/img/@src").get()            
                if status:
                    continue
                room_name = item.xpath(".//span[@class='type']/text()").get().strip()
                
                square_meters = item.xpath("//p[@class='jieshaocon']/span[1]/text()").extract_first()
                if square_meters:
                    item_loader.replace_value('square_meters', square_meters.split("m")[0])
                rent_mountly = item.xpath("//div[@class='abtroom_item']/div[@class='wq_item_mid']/div/div[@class='wq_item_right']/span/span[@class='pw']/text()").extract_first()
                if rent_mountly:
                    rent_mountly = rent_mountly.split('£')[-1].strip()
                    item_loader.replace_value('rent', int(rent_mountly)*4)
                else:
                    item_loader.replace_value('rent_string', "//span[@class='p3']/span/text()[normalize-space()]")

                item_loader.replace_value('external_link', f"https://www.ucnest.com/accommodation/UK/London/BuxtonCourtD/323/overview_1/"+"#"+room_name.replace("Room","").strip())         
          
                item_loader.replace_value('property_type', "room")
 
                item_loader.replace_value('title', f"{title} - {room_name}")
                
                item_loader.replace_value('room_count', '1')
                yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return "room"
