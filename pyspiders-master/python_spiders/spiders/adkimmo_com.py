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
    name = 'adkimmo_com'
    execution_type='testing'
    country='belgium'
    locale='fr'

    def start_requests(self):
        url = "https://www.adkimmo.com/a-louer/"
        yield Request(url,
                    callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='estates-listing']/li/a"):
            follow_url = response.urljoin(item.xpath("./@href").extract_first())
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Adkimmo_PySpider_belgium")
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")

        prop = response.xpath("//h1[@class='bluebarinfos__name']/strong/text()").extract_first()
        if "appartement" in prop:
            item_loader.add_value("property_type", "apartment")
        elif "studio" in prop:
            item_loader.add_value("property_type", "studio")
        else:
            return

        rent = "".join(response.xpath("//div[@class='bluebarinfos__price']/text()").extract())
        if rent:
            item_loader.add_value("rent", rent.replace(" ",""))
        item_loader.add_value("currency", "EUR")
        item_loader.add_xpath("latitude","substring-before(substring-after(//script[@type='text/javascript']/text()[contains(.,'var cLat')],'cLat = '),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(//script[@type='text/javascript']/text()[contains(.,'var cLat')],'cLong = '),';')")
        
        
        address = response.xpath("//h2[contains(@class,'reservation__title')]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            if "," in address:
                city = address.split(",")[-1].strip().split(" ")[0]
                zipcode = address.split(",")[-2].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)

                
        room_count = "".join(response.xpath("//th[contains(.,'Chambre')]/following-sibling::td/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
            
        square_meters = "".join(response.xpath("//th[contains(.,'Superficie')]/following-sibling::td/text()").getall())
        if square_meters:
            s_meters =  square_meters.replace(",",".")
            item_loader.add_value("square_meters", int(float(s_meters)))
            
        bathroom_count = "".join(response.xpath("//th[contains(.,'Salle')]/following-sibling::td/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        external_id = "".join(response.xpath("//div[@class='estate-text-reservation__ref']/text()").getall())
        if external_id:
            ex_id = external_id.split(":")[1].strip()
            if ex_id:
                item_loader.add_value("external_id", ex_id.strip())
        
        floor = "".join(response.xpath("//th[contains(.,'Etage')]/following-sibling::td/text()").getall())
        if floor:
            item_loader.add_value("floor", floor.strip())   
            
        energy_label = response.xpath("substring-before(substring-after(//div[@class='estate-fasticons__peb']/img/@src,'peb_'),'.')").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.upper())   
            
        parking = response.xpath("//th[contains(.,'Garage')]/following-sibling::td/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)    
            
        elevator = response.xpath("//td[contains(.,'Ascenseur')]/following-sibling::td/text()").get()
        if elevator:
            if "Oui" in elevator or elevator !="0":
                item_loader.add_value("elevator", True) 
            else:
                item_loader.add_value("elevator", False)
        
        deposit = response.xpath("//td[contains(.,'Garantie')]/following-sibling::td/text()").get()
        if deposit:
            deposit = int(deposit)*int(float(rent))
            item_loader.add_value("deposit", deposit) 
        
        utilities = response.xpath("//td[contains(.,'Charge')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities) 
            
        terrace = response.xpath("//td[contains(.,'Terrasse')]/following-sibling::td/text()[.!='0']").get()
        if terrace:
            item_loader.add_value("terrace", True) 
        
        furnished = response.xpath("//th[contains(.,'Meublé')]/following-sibling::td/text()").get()
        if furnished:
            if "Oui" in furnished:
                item_loader.add_value("furnished", True)
            
        desc = " ".join(response.xpath("//div[@class='ctext']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description",desc)
        
        if "Libre le" in desc:
            available_date = desc.replace("!",".").split("Libre le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//ul[@class='grand_slider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "ADK IMMO")
        item_loader.add_value("landlord_email", "info@adkimmo.com")
        item_loader.add_value("landlord_phone", "04/220 60 30")
      
        yield item_loader.load_item()