# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser 

class MySpider(Spider):
    name = 'cardensestateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cardensestateagents.co.uk/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Apartment",
                    "https://www.cardensestateagents.co.uk/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Terraced"
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.cardensestateagents.co.uk/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=House",
                    "https://www.cardensestateagents.co.uk/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Town+House"
                ],
                "property_type" : "house"
            },   
            {
                "url" : [
                    "https://www.cardensestateagents.co.uk/search/?instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=Studio+Apartment",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='property-main-image']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            p_url = response.url.split("search/")[0] + f"search/{page}.html?" + response.url.split("?")[1]
            yield Request(p_url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), "page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Cardensestateagents_Co_PySpider_united_kingdom")

        external_id = response.url.split('property-details/')[-1].split('/')[0].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//span[@class='light-text']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if 'exeter' in address.strip().lower():
                item_loader.add_value("city", 'Exeter')
            else:
                item_loader.add_value("city",address.split(",")[-1].replace("\t","").replace("\n",""))
            
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace('\xa0', ''))

        description = " ".join(response.xpath("//div[@class='col-sm-12 col-md-8']/p//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//img[contains(@src,'bed-')]/following-sibling::strong[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//img[contains(@src,'bath-')]/following-sibling::strong[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//br/following-sibling::text()[contains(.,'pppw') or contains(.,'pcm') or contains(.,'per week')]").get()
        if rent:
            if 'pppw' in rent or 'per week' in rent:
                rent = rent.split('£')[-1].split('pppw')[0].split('per')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
            elif 'pcm' in rent:
                rent = rent.split('£')[-1].split('pcm')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent))))
                item_loader.add_value("currency", 'GBP')
        
        available_date = response.xpath("//br/following-sibling::text()[contains(.,'Availability:')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(':')[-1].strip(), date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//br/following-sibling::text()[contains(.,'Deposit:')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('£')[-1].strip())
        else:
            deposit = response.xpath("//div[@class='col-sm-12 col-md-8']/p[contains(.,'Deposit')][1]/text()").extract_first()
            
            if deposit:
                item_loader.add_value("deposit", deposit.split('£')[1].strip())
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-carousel']/div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        latitude_longitude = response.xpath("//script[contains(.,'ShowMap')]/text()").get()
        if latitude_longitude:
            item_loader.add_value("latitude", latitude_longitude.split('&q=')[-1].split('%2C')[0].strip())
            item_loader.add_value("longitude", latitude_longitude.split('&q=')[-1].split('%2C')[-1].split('"')[0].strip())

        energy_label = response.xpath("//p[contains(text(), 'EPC:')]/text()").get()
        if energy_label:
            energy_label = energy_label.split(':')[-1].strip()
            if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)

        pets_allowed = response.xpath("//p[contains(text(), 'no Pets')]/text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)

        furnished = response.xpath("//p[contains(., 'Type:') and contains(.,'Unfurnished')]").get()
        if furnished:
            item_loader.add_value("furnished", False)
        else:
            furnished = response.xpath("//p[contains(., 'Type:') and contains(.,'Furnished')]").get()
            if furnished:
                item_loader.add_value("furnished", True)
            
        washing_machine = response.xpath("//p[contains(., 'Appliances:') and contains(.,'Washing Machine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", "Cardens Estate Agents")
        item_loader.add_value("landlord_phone", '01392 433866')
        item_loader.add_value("landlord_email", 'lettings@cardensestateagents.co.uk')

        yield item_loader.load_item()
