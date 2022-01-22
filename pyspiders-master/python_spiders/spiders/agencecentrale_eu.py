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
    name = 'agencecentrale_eu'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agencecentrale_PySpider_france_fr'
    headers = {
        'content-type': "application/x-www-form-urlencoded",
        'accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        'accept-encoding': "gzip, deflate, br",
        'accept-language': "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        'cache-control': "max-age=0",
        'origin': "https://www.agencecentrale.eu",
        'referer': "https://www.agencecentrale.eu/",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
        }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.agencecentrale.eu/location/", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='center2']"):
            f_url=response.urljoin(item.xpath("./span[@class='linkPG']/a/@href").get())
            prop_type=item.xpath("./h2/text()").get()
            if "APPARTEMENT" in prop_type:
                property_type="Apartment"
            elif "MAISON" in prop_type:
                property_type="house"
            else: property_type=None
            
            yield Request(f_url, callback=self.populate_item, method="POST", headers=self.headers, meta={'property_type' :property_type, "external_link": f_url},)
                                
        next_page=response.xpath("//span/a[@rel='next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page), 
                callback=self.parse, 
                meta={"property_type" : response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Agencecentrale_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//h1[@class='txtb fs20px']/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.meta.get("external_link"))
        external_id = response.url
        if external_id:
            external_id = external_id.split('location/')[-1].split('/',1)[-1].split('/',1)[-1].split('/')[0].strip()
            item_loader.add_value("external_id", external_id)
        
        rent="".join(response.xpath("//div[@class='large-4 columns']/div/strong[contains(.,'€')]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.split('€')[0].strip())
        item_loader.add_value("currency", "EUR")
        
        attr=response.xpath("//div[contains(@class,'sl_cl6 txtb')]/text()").get()
        if "m2" in attr:
            item_loader.add_value("square_meters", attr.split('-')[0].split('m2')[0].strip())
        
        if "pièce" in attr:
            room_count=attr.split('-')[1].strip().split(' ')[0]
            if room_count!='0':
                item_loader.add_value("room_count", room_count )


        bathroom_count = response.xpath("//div[contains(@class,'fs14px')]/strong[contains(.,'Nombre de salle')]/following-sibling::text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip() )

        balcony = response.xpath("//div[contains(@class,'fs14px')]/strong[contains(.,'Balcon ')]/following-sibling::text()").extract_first()
        if balcony:
            if "oui" in balcony.lower():
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)

        terrace = response.xpath("//div[contains(@class,'fs14px')]/strong[contains(.,'Terrasse ')]/following-sibling::text()[1]").extract_first()
        if terrace:
            if "oui" in terrace.lower():
                item_loader.add_value("terrace", True)
            else:
                item_loader.add_value("terrace", False)

        swimming_pool = response.xpath("//div[contains(@class,'fs14px')]/strong[contains(.,'Terrasse ')]/following-sibling::text()[1]").extract_first()
        if swimming_pool:
            if "oui" in swimming_pool.lower():
                item_loader.add_value("swimming_pool", True)
            else:
                item_loader.add_value("swimming_pool", False)

        zipcode = response.xpath("//div[contains(@class,'medium-12 large-8')]/h1/text()").extract_first()
        if zipcode:
            zipcode = zipcode.split("(")[1].split(")")[0].strip()
            item_loader.add_value("zipcode", zipcode.strip() )
        
        address=response.xpath("//span[contains(@class,'cityy')]//text()").get()
        if address:
            item_loader.add_value("address","{} {}".format(address,zipcode))
            item_loader.add_value("city", address.strip())

        desc="".join(response.xpath("//div[contains(@class,'fs13px lh20'    )]/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[contains(@class,'small_pics')]//div/center/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_xpath("landlord_name","//div[contains(@class,'mgt25')]/text()")
        item_loader.add_xpath("landlord_phone","//div[contains(@class,'fs25px')]/text()")

        yield item_loader.load_item()