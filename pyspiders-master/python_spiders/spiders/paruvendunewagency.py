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
    name = 'paruvendunewagency'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="ParuvenduNewAgency_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {"url": "https://www.paruvendu.fr/immobilier/pro/the-new-agency-chateau-thierry-02-75738/annonces/?idMinisite=75738&tri=&rfam=ILH00000&tb=tbMai%7CtbVil%7CtbCha%7CtbPro%7CtbHot%7CtbMou%7CtbFer&sur0=&nbp0=&nbp1=&px1=&fulltext=&codeINSEE=&lo=&idParticulier=0&codeRubrique=&p=1&nombreTotalAnnonces=50&ajax=true&_=1605785228157", "property_type": "house"},
            {"url": "https://www.paruvendu.fr/immobilier/pro/the-new-agency-chateau-thierry-02-75738/annonces/?idMinisite=75738&tri=&rfam=ILH00000&tb=tbApp%7CtbDup%7CtbChb%7CtbLof%7CtbAtl%7CtbPla&sur0=&nbp0=&nbp1=&px1=&fulltext=&codeINSEE=&lo=&idParticulier=0&codeRubrique=&p=1&nombreTotalAnnonces=50&ajax=true&_=1605785194910", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING 
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='vit15_annoncedeliste2']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            if response.meta.get("property_type") == "apartment":
                url = f"https://www.paruvendu.fr/immobilier/pro/the-new-agency-chateau-thierry-02-75738/annonces/?idMinisite=75738&tri=&rfam=ILH00000&tb=tbApp%7CtbDup%7CtbChb%7CtbLof%7CtbAtl%7CtbPla&sur0=&nbp0=&nbp1=&px1=&fulltext=&codeINSEE=&lo=&idParticulier=0&codeRubrique=&p={page}&nombreTotalAnnonces=50&ajax=true&_=1605785194910"
            elif response.meta.get("property_type") == "house":
                url = f"https://www.paruvendu.fr/immobilier/pro/the-new-agency-chateau-thierry-02-75738/annonces/?idMinisite=75738&tri=&rfam=ILH00000&tb=tbMai%7CtbVil%7CtbCha%7CtbPro%7CtbHot%7CtbMou%7CtbFer&sur0=&nbp0=&nbp1=&px1=&fulltext=&codeINSEE=&lo=&idParticulier=0&codeRubrique=&p={page}&nombreTotalAnnonces=50&ajax=true&_=1605785228157"
            yield Request(url, callback=self.parse, meta={"page": page+1, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)
        dontallow=response.xpath("//div[@class='vit17-vendu']/span/text()").get()
        if dontallow and "Loué"==dontallow:
            return 
        
        external_id = "".join(response.xpath("//ul[@class='vp16infoscles']/li[1]/text()").extract())
        item_loader.add_value("external_id", external_id.strip())
        
        title = response.xpath("//h1/text()").get()
        title = re.sub('\s{2,}', ' ', title)
        item_loader.add_value("title", title)

        desc="".join(response.xpath("//div[@class='vit15zoneannoncesdetail']/p/text()").extract())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))
        desc=desc.replace("magnifiques","").replace("grandes","").replace("autres","")
        
        square_meters=response.xpath("//title/text()").extract_first()
        if "m²" in square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].split(" ")[-2])
        elif "m2" in desc:
            sq_mt=desc.split("m2")[0].strip().split(" ")[-1]
            if sq_mt=="000":
                sq_mt=desc.split("m2")[1].strip().split(" ")[-1]
            item_loader.add_value("square_meters", sq_mt)
        
 
        rent_value = ""    
        rent="".join(response.xpath("normalize-space(//div[@class='flor']/p)").extract())
        if rent:
            rent_value = rent.replace(" ","").split("€")[0].strip()
            item_loader.add_value("rent_string",rent.replace(" ",""))
        room_count=item_loader.get_output_value("description")
        if room_count:
            room=room_count.split(" chambres")[0].split(" ")[-1]
            if room:
                item_loader.add_value("room_count",room)
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room=item_loader.get_output_value("description")
            if room:
                item_loader.add_value("room_count",room.split(" pièces")[0].split(" ")[-1])
            roomcheck=item_loader.get_output_value("room_count")
            if not roomcheck:
                room=response.xpath("//title//text()").get()
                if room:
                    item_loader.add_value("room_count",room.split(" pi")[0].split(" ")[-1])

        if "meublée" in desc:
            item_loader.add_value("furnished", True)
        address=response.xpath("normalize-space(//div[@class='vit15titreref']/h2/text())").extract_first()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])
        
        deposit = response.xpath("//span[contains(.,'de garantie')]/strong/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("€","").strip())
        
        label="".join(response.xpath("//div[@class='DPE_graphBG']/div/@class").extract())
        if label:
            item_loader.add_value("energy_label", label.split("C")[1])
        
        images=[x for x in response.xpath("//div[@class='blocselecimg-miniatv2']/figure/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_phone", "09.80.80.07.71")
        item_loader.add_value("landlord_name", "ParuVendu")
        
        status=response.xpath("//div/span[contains(.,'Vendu')]/text()").get()
        if status==None and rent_value and int(rent_value) < 50000:
            yield item_loader.load_item()