# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'terreetpierre_fr'    
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"apartment":"1", "house":"2"}

        for key, value in kwargs.items():
            formdata = {
                "typop": "2",
                "selectedCp": "",
                "exclu": "",
                "site_frm_select_type": value,
                "site_frm_select_budget": "",
                "site_frm_pieces": "",
                "site_frm_ref": "",
            }
            yield FormRequest("https://www.terreetpierre.fr/results",
                            callback=self.parse,
                            formdata=formdata,
                            meta={'property_type': key, "selected_type":value})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1)

        seen = False
        for item in response.xpath("//div[@class='type']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        
        if page == 1 or seen:
            selected_type = response.meta["selected_type"]
            p_url = f"https://www.terreetpierre.fr/index.php?numpage={page}&tri=&op=results&typop=2&selectedCp=&exclu=&site_frm_select_type={selected_type}&site_frm_select_budget=&site_frm_pieces=&site_frm_ref="
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "page":page+1, "selected_type":selected_type}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Terreetpierre_PySpider_france")
        item_loader.add_xpath("external_id", "//div[@class='details']//li[span[.='Référence']]/span[@class='value']/text()")
         
        item_loader.add_xpath("title", "//h1/span[@class='titre']//text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='details']//li[span[contains(.,'de bain') or contains(.,'Salles d')]]/span[@class='value']//text()[.!='0']")
        room_count = response.xpath("//div[@class='details']//li[span[contains(.,'chambre')]]/span[@class='value']//text()[.!='0']").extract_first()       
        if room_count:
            item_loader.add_value("room_count", room_count) 
        else:
            room_count = response.xpath("substring-before(//h1/span[@class='titre']//text()[contains(.,'pièce')],'pièce')").extract_first()       
            if room_count:
                room_count = room_count.strip().split(" ")[-1]
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count) 
        rent = response.xpath("//h2[@class='main-price-detail']//text()[normalize-space()]").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent)   
     
        address = response.xpath("//h1/span[@class='situation']//text()").extract_first()       
        if address:
            item_loader.add_value("address", address)   
            item_loader.add_value("city", address.split("(")[0].strip())   
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0].strip())   
        
        square = response.xpath("substring-before(//div[@class='details']//li[span[.='Surface habitable']]/span[@class='value']//text(),'m')").extract_first()       
        if square:
            item_loader.add_value("square_meters", square) 
        utilities = response.xpath("//div[@class='text description']//p//text()[contains(.,'charge ') and contains(.,'€')]").extract_first()       
        if utilities:
            utilities =utilities.split("€")[0].strip().split(" ")[-1].replace(",",".").strip()
            item_loader.add_value("utilities",int(float(utilities)) ) 
        energy = response.xpath("//div[@class='zone-content']/div/img[contains(@src,'dpe') and not(contains(@src,'_np'))]/@src").extract_first()       
        if energy:
            item_loader.add_value("energy_label", energy.split("_")[1].split(".")[0]) 
  
        desc = " ".join(response.xpath("//div[@class='text description']//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//div[@class='detail-section photos']//a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)    

        item_loader.add_value("landlord_phone", "04 90 14 14 84")
        item_loader.add_value("landlord_email", "location@terreetpierre.fr")
        item_loader.add_value("landlord_name", "TERRE & PIERRE")    
        # yield item_loader.load_item()

