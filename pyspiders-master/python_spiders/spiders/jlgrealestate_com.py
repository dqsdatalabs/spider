# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser
from word2number import w2n

class MySpider(Spider):
    name = 'jlgrealestate_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "Jlgrealestate_PySpider_netherlands_nl"

    def start_requests(self):
        start_urls = [
            {"url": "https://jlgrealestate.com/woningen/huur"}
        ]  # LEVEL 1
        
        for url in start_urls: 
            yield Request(url=url.get('url'),
                            callback=self.parse)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen=False
        for item in response.xpath("//div[@id='entity-items']/article/div[@class='card__inner']/a/@href").getall():
            follow_url = response.urljoin(item)
            seen=True
            yield Request(follow_url, callback=self.populate_item)
        if page == 2 or seen:
            url = f"https://jlgrealestate.com/woningen/huur/page/{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)

        # lat = response.meta.get("lat")
        # lng = response.meta.get("lng")
        
        prop ="".join(response.xpath("//div[@class='prose clean']//p/text()").getall())
        if prop and "apartment" in prop.lower():
           item_loader.add_value("property_type","apartment")
        elif prop and "house" in prop.lower():
            item_loader.add_value("property_type", "house")
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//div[contains(@class,'woning__heading')]/h2/text()").get()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        urlcheck=response.url
        if urlcheck and "koop" in urlcheck:
            return 
        # ext_id = "".join(filter(str.isnumeric, response.url))
        # if ext_id:
        #     item_loader.add_value("external_id", ext_id) 
 
        # item_loader.add_value("latitude", lat)
        # item_loader.add_value("longitude", lng)

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            external_id = external_id.split("p=")[-1]
            item_loader.add_value("external_id",external_id)

        price =response.xpath("//div[@class='woning__prijs inline m25b']/p/text()").getall()
        if price:
            item_loader.add_value("rent_string", price)
        if not price:
            return 
 
        position = response.xpath("//script[contains(text(),'latitude')]").get()
        if position:
            lat = re.search('latitude":([\d.]+),', position).group(1)
            long = re.search('longitude":([\d.]+)', position).group(1)
            item_loader.add_value("latitude",lat)
            item_loader.add_value("longitude",long)
        
        square = response.xpath("//span/text()[contains(.,'Woonoppervlakte')]/../following-sibling::text()").getall()
        if square:
            item_loader.add_value("square_meters", square)

        
        adress = response.xpath("//div[contains(@class,'woning__heading')]/h2/text()").get()
        adress+=' '+response.xpath("//div[contains(@class,'woning__adres')]/p/text()").get()
        if adress:
            item_loader.add_value("address",adress)
        city=response.xpath("//div[contains(@class,'woning__adres')]/p/text()").get()
        if city:
            city=city.split(",")[-1].strip()
            item_loader.add_value("city",city)
        zipcode=response.xpath("//div[contains(@class,'woning__adres')]/p/text()").get()
        if zipcode:
            zipcode=zipcode.split(",")[0]
            item_loader.add_value("zipcode",zipcode)

                

        room_count =response.xpath("//span/text()[contains(.,'Aantal slaapkamers')]/../following-sibling::text()").getall()
        if room_count:
            item_loader.add_value("room_count", room_count)
      

        images = [response.urljoin(x)for x in response.xpath("//figure[@class='woning__gallery__figure ']/a/@href").getall()]
        if images:
                item_loader.add_value("images", images)

        # energy_label = "".join(response.xpath("normalize-space(//text()[contains(.,'Energy label')])").extract())
        # if energy_label:
        #     energy_label = energy_label.split(" label ")[-1].strip()
        #     if energy_label:
        #         item_loader.add_value("energy_label", energy_label)
        # else:
        #     label = "".join(response.xpath("normalize-space(//text()[contains(.,'energy label')])").extract())
        #     if label:
        #         item_loader.add_value("energy_label", label.split(" label ")[-1].strip())
       

        desc = response.xpath("//div[@class='prose clean']//p//text()").getall()
        if desc:
            item_loader.add_value("description", desc)

        
        # utilities = response.xpath("//div[@class='col-xs-12 col-md-8 house-content']/p/text()[contains(.,'Prepaid amount ')]").extract_first()
        # if utilities:
        #     item_loader.add_value("utilities", utilities)
        # deposit = response.xpath("//div[@class='col-xs-12 col-md-8 house-content']/p/text()[(contains(.,'deposit') or contains(.,'Deposit')) and contains(.,'month')]").extract_first()
        # if deposit and price:
        #     deposit_val = "".join(filter(str.isnumeric, deposit))
        #     price = "".join(filter(str.isnumeric, price))
        #     if deposit_val: 
        #         item_loader.add_value("deposit", int(deposit_val) * int(price))
        #     else:   
        #         try:
        #             deposit = deposit.split(" month")[0].split(" ")[-1]    
        #             deposit_val = w2n.word_to_num(deposit)
        #             item_loader.add_value("deposit", int(deposit_val) * int(price))
        #         except:
        #             pass               

        # available_date = response.xpath("substring-after(//div[@class='col-xs-12 col-md-8 house-content']/p/text()[contains(.,'starting from')],'starting from')").get()
        # if available_date:
        #     try:           
        #         new_date = dateparser.parse(available_date.strip(), languages=['en']).strftime("%Y-%m-%d")
        #         item_loader.add_value("available_date", new_date)
        #     except:
        #         pass
        parking = "".join(response.xpath("//div[@class='prose clean']//p/text()").getall())
        if parking and "parking" in parking.lower():
            item_loader.add_value("parking", True)

        furnished = "".join(response.xpath("//div[@class='prose clean']//p/text()").getall())
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        elevator ="".join(response.xpath("//div[@class='prose clean']//p/text()").getall())
        if elevator and "elevator" in elevator.lower():
            item_loader.add_value("elevator", True)

        item_loader.add_value("landlord_phone", "+31 20 33 000 31")
        item_loader.add_value("landlord_email", "info@jlg.nl")
        item_loader.add_value("landlord_name", "Mascha Nijssen")

        yield item_loader.load_item()