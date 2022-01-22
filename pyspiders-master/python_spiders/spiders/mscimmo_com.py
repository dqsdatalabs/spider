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

class MySpider(Spider):
    name = 'mscimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Mscimmo_PySpider_france_fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.msc-immo.com/fr/annonces/locations-p-r70-3-1.html#menuSave=3&page=1&RgpdConsent=1603016681313&TypeModeListeForm=text&ope=2&filtre=8", "property_type": "house"},
            {"url": "https://www.msc-immo.com/fr/annonces/locations-p-r70-3-1.html#menuSave=3&page=1&RgpdConsent=1603016681313&TypeModeListeForm=text&ope=2&filtre=2", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for follow_url in response.css("div.liste-bien-ribbon > a::attr(href)").extract():
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Mscimmo_PySpider_"+ self.country + "_" + self.locale)

        #title = " ".join(response.xpath("//div[@class='detail-bien-title']/*[not(contains(.,'Ref'))]").extract())
        title = response.xpath("//li[@class='detail-side-prix']//text()").get()
        t1 = response.xpath("//li[@class='detail-side-type']/text()").get()
        t2 = response.xpath("//li[@class='detail-side-ville']/text()").get()
        title = t1 + " " + t2 + " " + title
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        item_loader.add_value("external_link", response.url)       
        
        external_id =  "".join(response.xpath("//div[@class='detail-bien-title']//div[2]/text()").extract())
        item_loader.add_xpath("external_id",external_id.strip() )
        
        square_mt=response.xpath("//ul[@class='nolist']/li[5]/text()").get()
        item_loader.add_value("square_meters", square_mt )
        
        room_cnt=response.xpath("//ul[@class='nolist']/li[contains(.,'pièce')]/text()").get()
        if room_cnt:
            room_cnt = room_cnt.strip()
            item_loader.add_value("room_count", room_cnt.split("pièce(s)")[0].strip() )
        
        desc="".join(response.xpath("//div[@class='detail-bien-desc-content clearfix']/p/text()").extract())
        item_loader.add_value("description", desc)
        
        if "salle" in desc.lower():
            bathroom=desc.lower().split("salle")[0].strip().split(" ")[-1]
            if "une" in bathroom.lower():
                item_loader.add_value("bathroom_count", "1")

        if "étage" in desc.lower():
            floor=desc.lower().split("étage")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        if "laver" in desc.lower():
            item_loader.add_value("washing_machine", True)
        
        furnished=response.xpath(
            "//div[@class='detail-bien-desc-content clearfix']/p/text()[contains(.,' meublé')]").get()
        if furnished:
            item_loader.add_value("furnished" , True)
        
        latitude=response.xpath("//ul/li[contains(@class,'lat')]/text()").get()
        longitude=response.xpath("//ul/li[contains(@class,'lng')]/text()").get()
        if latitude and longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        deposit="".join(response.xpath("//li[@class='hidden prix-location']/ul[@class='nolist']/li[4]/span[2]/text()").extract())
        depo = deposit.strip().replace(" ","")
        item_loader.add_value("deposit", int(float(depo)))
        
        energy_l=response.xpath("//div[contains(@class,'detail-bien-dpe')]/img/@src").get()
        if energy_l:
            try:
                energy_l=energy_l.split("nrj-w-")[1].split("-")[0]
            except IndexError:
                energy_l=False
            if energy_l:
                item_loader.add_value("energy_label", energy_l)
        
        attr=response.xpath("//div[@class='detail-bien-title']//h2/text()").get()
        item_loader.add_value("zipcode",attr.split(" ")[1].split("(")[1].split(")")[0])
        item_loader.add_value("city",attr.split(" ")[0])
        item_loader.add_value("address",attr.split(" ")[0])
        
        images=[x for x in response.xpath("//div[@class='thumbs-flap-container']//div/img/@src[.!='https://www.msc-immo.com/images/vide_detail_mini.jpg']").getall()]
        if images:
            item_loader.add_value("images",images)
            
        utilitie="".join(response.xpath("normalize-space(//span[@class='cout_charges_mens']/text()[1])").extract())
        item_loader.add_value("utilities", utilitie)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        rent="".join(response.xpath("normalize-space(//div[@class='detail-side-prix-container']/li[1]/text())").extract())
        if rent:
            rent=rent.replace(" ","")
            if "." in rent:
                number = float(re.sub('[^.\-\d]', '', rent))
                new_rent=int(number)
                rent=rent.replace(str(number),str(new_rent))
            item_loader.add_value("rent_string",rent)
        
        item_loader.add_value("landlord_phone","04 99 06 1000")
        item_loader.add_value("landlord_name","MAS SAINT COME IMMOBILIER")
        yield item_loader.load_item()