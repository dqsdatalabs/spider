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
    name = 'gillocationmelun_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        formdata = {
            "od_group_offredem_separe": "2",
            "od_group_bien_separe": "2",
            "prix": "",
            "localisation": "",
            "piece": "",
            "surface": "",
            "ref2": "Référence :",
            "ref": "",
            "_": "",
        }
        
        yield FormRequest(
            "http://gil-locationmelun.com/xmlrpcnet.php?mode=chargeannonces",
            callback=self.parse,
            formdata=formdata,
            meta={
                "property_type":"apartment"
            }
        )

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        for item in response.xpath("//div[@id='allliste']/div[@class='boxlistingdesbiens']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='>']/@href").get()
        if next_page:
            p_url = f"http://gil-locationmelun.com/xmlrpcnet.php?mode=page_pagination_recherche&od=&pagepagination={page}"
            yield Request(
                url=p_url,
                callback=self.parse,
                meta={
                    "page" : page+1,
                    'property_type': response.meta.get('property_type')
                }
            )
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Gillocationmelun_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//span[@class='pager_libelle1']/text()")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        rent=response.xpath("//div[@class='annonce_add']/span[contains(.,'Loyer')]//following-sibling::span/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)
        
        square_meters=response.xpath("//div[@class='annonce_add']/span[contains(.,'habitable')]//following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        
        room_count=response.xpath("//div[@class='annonce_add']/span[contains(.,'pièce')]//following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        zipcode = response.xpath("//span[contains(.,'Code postal')]/following-sibling::span/text()").get()
        city = response.xpath("//span[contains(.,'Ville')]/following-sibling::span/text()").get()
        if zipcode and city:
            item_loader.add_value("address", city.strip() + ' (' + zipcode.strip() + ')')
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city.strip())
        
        latitude= response.xpath("//input[@id='elgmaps-lat']/@value").get()
        longitude= response.xpath("//input[@id='elgmaps-lng']/@value").get()
        if latitude and longitude:
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        parking = response.xpath("//span[contains(.,'Parking')]/following-sibling::span/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                item_loader.add_value("parking", True)
            elif int(parking.strip()) == 0:
                item_loader.add_value("parking", False)

        balcony = response.xpath("//span[contains(.,'Ascenseur')]/following-sibling::span/text()").get()
        if balcony:
            if balcony.strip().lower() == 'oui':
                item_loader.add_value("balcony", True)
            elif balcony.strip().lower() == 'non':
                item_loader.add_value("balcony", False)
        
        bathroom_count = response.xpath("//span[contains(.,'Salle de bain')]/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
            
        external_id=response.xpath("//span[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        desc="".join(response.xpath("//div[@id='detail_bien_corps']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[@class='container']/ul/li/div/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","GIL GESTION")
        item_loader.add_value("landlord_phone","01 64 10 64 10")
        
        floor=response.xpath("//div[@class='annonce_add']/span[contains(.,'Etage')]//following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        utilities=response.xpath("//div[@class='annonce_add']/span[contains(.,'Charges')]//following-sibling::span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0].strip())
        
        deposit=response.xpath("//div[@class='annonce_add']/span[contains(.,'garantie')]//following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip())
        
        yield item_loader.load_item()