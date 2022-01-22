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
    name = 'ablocationimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Ablocationimmobilier_PySpider_france_fr'
    custom_settings= {"HTTPCACHE_ENABLED":False}
    def start_requests(self):
        start_urls = [
            {"url": "https://ablocation-immobilier.com/advanced-search/?filter_search_action%5B%5D=location&filter_search_type%5B%5D=maison&advanced_city=&advanced_area=&advanced_rooms=&advanced_bath=&price_low=&price_max=&submit=LANCER+LA+RECHERCHE", "property_type": "house"},
            {"url": "https://ablocation-immobilier.com/advanced-search/?filter_search_action%5B%5D=location&filter_search_type%5B%5D=apartments&advanced_city=&advanced_area=&advanced_rooms=&advanced_bath=&price_low=&price_max=&submit=LANCER+LA+RECHERCHE", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing_ajax_container']//div[contains(@class,'property_listing')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Ablocationimmobilier_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//h1[@class='entry-title entry-prop']/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//div[@class='panel-body']/div[contains(.,'Prix')]/text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        square_meters=response.xpath("//div[@class='panel-body']/div[contains(.,'Surface')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())
        
        room_count = response.xpath("//strong[contains(.,'Chambres:')]/following-sibling::text()").get()
        if room_count: item_loader.add_value("room_count", room_count)
        
        address=response.xpath("//span[@class='adres_area']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        latitude = response.xpath("//div[@id='googleMapSlider']/@data-cur_lat").get()
        longitude = response.xpath("//div[@id='googleMapSlider']/@data-cur_long").get()
        if latitude or longitude:
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        external_id=response.xpath("//div[@class='panel-body']/div[contains(.,'Réf')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        bathroom_count="".join(response.xpath("//div[@class='listing_detail col-md-4'][contains(.,'Salles de bains:')]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        utilities = response.xpath("//text()[contains(.,'Provisions sur charges') or contains(.,'Provision sur charges')]").get()
        if utilities:
            item_loader.add_value("utilities", "".join(filter(str.isnumeric, utilities)))

        desc="".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//ol[contains(@id,'carousel-indicators')]/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name","A.B LOCATİON")
        item_loader.add_value("landlord_phone","05.59.43.90.97")
        item_loader.add_value("landlord_email","ab.location.64@wanadoo.fr")
        
        energy_label=response.xpath("//div[@class='panel-body']/div[contains(.,'DPE')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split('/')[0])
            
        deposit=response.xpath("//div[@class='panel-body']/div[contains(.,'garantie')]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip())
        
        elevator=response.xpath("//div[@class='panel-body']/div/i[contains(@class,'check')]//parent::div/text()[contains(.,'Ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator",True)
        
        parking=response.xpath("//div[@class='panel-body']/div/i[contains(@class,'check')]//parent::div/text()[contains(.,'Parking')]").get()
        garage=response.xpath("//div[@class='panel-body']/div/i[contains(@class,'check')]//parent::div/text()[contains(.,'Garage')]").get()
        if parking or garage:
            item_loader.add_value("parking",True)

        status = response.xpath('//span[@class="price_label"]/text()').get()
        if 'vente' in desc.lower() and 'mois' not in status.lower():
            return
        else:
            yield item_loader.load_item()