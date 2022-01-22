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
    name = 'fr_foncia_com'
    start_urls = ["https://fr.foncia.com/location"]

    def parse(self, response):
        
        for item in response.xpath("//div[@class='TopVilles-cols']//li/a/@href").extract():
            if "maison" in item:
                prop_type = "house"
            elif "appartement" in item:
                prop_type = "apartment"
            else:
                prop_type = None
            follow_url = response.urljoin(item)
            yield Request(follow_url,
                            callback=self.jump,
                            meta={'property_type': prop_type})
            


    # 1. FOLLOWING
    def jump(self, response):
        
        for item in response.xpath("//h3[@class='TeaserOffer-title']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
        next_page = response.xpath("//a[.='Suivante >']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.jump,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        with open("debug","wb") as f:f.write(response.body)
        prop_type = response.meta.get('property_type')
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return

        item_loader.add_value("external_link", response.url)
        
        rent=response.xpath("normalize-space(//p[@class='OfferTop-price']/text())").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        else: return
        square_meters=response.xpath("//p[@class='MiniData-item'][contains(.,'m2')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        else: return
        
        room_count=response.xpath("normalize-space(//p[@class='MiniData-item'][contains(.,'pièce')]/text())").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        else: return
        
        address = response.xpath("//p[@class='OfferTop-loc']/text()[2]").get()
        if address:
            address = address.strip().replace(' ', '').replace('\n', ' ').replace('(', ' (')
            zipcode = address.split('(')[1].split(')')[0].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", zipcode)
        else: return
        
        external_id=response.xpath("//div[@class='OfferDetails']/section/p/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1].strip())
        
        title=response.xpath("//div[@class='OfferTop-head']/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        desc="".join(response.xpath("//div[@data-widget='ToggleBlockMobile']//text()").getall())
        desc2="".join(response.xpath("//div[@class='OfferDetails-content']/p[1]/text()").getall())
        if desc or desc2:
            item_loader.add_value("description", desc.replace("\n","")+desc2)
        

        images=[x for x in response.xpath("//ul[@class='OfferSlider-main-slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        phone=response.xpath("//div[@class='OfferContact-content']/p/a/span[contains(.,'+')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        charges=response.xpath("normalize-space(//p[@class='OfferTop-mentions']/text())").get()
        if charges:
            item_loader.add_value("utilities", charges.split("€")[0].split("dont")[1].strip())
        
        deposit="".join(response.xpath("//div[@data-widget='ToggleBlockMobile-content']/ul/li[contains(.,'garantie')]//text()").getall())
        if deposit:
            item_loader.add_value("deposit",  deposit.split("€")[0].split("garantie")[1].strip().replace(" ","."))
        
        
        
        yield item_loader.load_item()
