# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
import math
import dateparser
import re
class MySpider(Spider):
    name = 'adelimo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.adelimo.com/location/maison?prod.prod_type=house", "property_type": "house"},
            {"url": "https://www.adelimo.com/location/appartement?prod.prod_type=appt", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='_ap0eae']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
       
        item_loader.add_value("external_source", "Adelimo_PySpider_"+ self.country + "_" + self.locale)

        price = response.xpath("//li//span[contains(text(),'LOYER')]/text()").get()
        if price:
            price = price.split("+")[0].split(":")[1].replace("€", "").strip().replace(" ", "")
            item_loader.add_value("rent", price)
            try:
                utilities = price.split("+")[1].split("€")[0].strip().replace(" ", "")
                if utilities:
                    item_loader.add_value("utilities", utilities)
            except:
                pass
        elif not price or not item_loader.get_collected_values("rent"):
            price = "".join(response.xpath("//p[@class='_1vj3l1e _5k1wy textblock ']/text()").extract())
            if price:
                item_loader.add_value("rent", price.replace(" ","").strip())
        item_loader.add_value("currency", "EUR")
        
        item_loader.add_xpath("floor", "normalize-space(//li/div[div[contains(.,'Étage')]]/div/span[2]/text())")
        item_loader.add_xpath("bathroom_count", "normalize-space(//li/div[div[contains(.,'Salle d')]]/div/span[2]/text())")
        item_loader.add_xpath("external_id", "normalize-space(//li/div[div[contains(.,'Référence')]]/div/span[2]/text())")
        item_loader.add_xpath("city", "normalize-space(//li/div[div[contains(.,'Localisation')]]/div/span[2]/text())")
        item_loader.add_xpath("address", "normalize-space(//li/div[div[contains(.,'Localisation')]]/div/span[2]/text())")
       
        square = response.xpath("//li/div[div[contains(.,'Surface')]]/div/span[2]/text()").extract_first()
        if square:
            item_loader.add_value("square_meters", square.strip())

        room_count = response.xpath("//li/div[div[contains(.,'Chambres')]]/div/span[2]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//li/div[div[contains(.,'Pièces')]]/div/span[2]/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        desc = "".join(response.xpath("//span[contains(@class,'_lz3dts _5k1wy')]/text()").extract())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())
           
        
        # floor = response.xpath("//div[@class='tech_detail']//td[contains(.,'Étage')]/following-sibling::td//text()").extract_first()
        # if floor:
        #     item_loader.add_value("floor",floor.strip() )
 
        deposit = response.xpath("substring-after(//span[contains(@class,'_1n82gp4 _5k1wy')]/text()[contains(.,'Dépôt de garantie')],'Dépôt de garantie ')").extract_first()
        if deposit :
            dep = deposit.split(".")[0].strip()
            item_loader.add_value("deposit",dep)

        utilities = response.xpath("substring-after(//span[contains(@class,'_1n82gp4 _5k1wy')]/text()[contains(.,'charges')],'charges ')").extract_first()
        if utilities :
            item_loader.add_value("utilities",utilities.split(".")[0].strip() )
               

        
        energy =response.xpath("//div[@class='dpe_container']//b[@class='dpe-letter-active']/text()").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy.split(":")[0].strip())
       
      
        furnished = response.xpath("normalize-space(//li/div[div[contains(.,'Ameublement')]]/div/span[2]/text())").extract_first()
        if furnished:
            if "Non meublé" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[@class='tech_detail']//td[contains(.,'Ascenseur')]/following-sibling::td//text()").extract_first()
        if elevator:
            if "Non" in elevator:
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        terrace = response.xpath("//li/div[div[contains(.,'terrasse')]]/div/span[2]/text()").extract_first()
        if terrace:
                item_loader.add_value("terrace", True)
        swimming_pool = response.xpath("normalize-space(//li/div[div[contains(.,'Piscine')]]/div/span[2]/text())").extract_first()
        if swimming_pool:
            if "Non" in swimming_pool:
                item_loader.add_value("swimming_pool", False)
            else:
                item_loader.add_value("swimming_pool", True)

        date2 = ""
        a_date = response.xpath("//span[contains(@class,'_lz3dts _5k1wy')]/strong[2]/text()").extract_first()
        if a_date:
            if ":" in a_date:
                date2 = a_date.split(":")[1].split(".")[0].strip()
            else:
                date2 = a_date.split(" ")[-1].strip().replace(".","")
            date_parsed = dateparser.parse(
                date2, date_formats=["%d-%m-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)
      
        images = [response.urljoin(x) for x in response.xpath("//div[@class='_wsusle image _1yfus1e']/img/@data-src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        
        item_loader.add_value("landlord_phone", "09 81 39 80 40")
        item_loader.add_value("landlord_name", "Adelimo Immobilier")

        yield item_loader.load_item()