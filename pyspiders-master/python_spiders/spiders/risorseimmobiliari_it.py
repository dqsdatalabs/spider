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
    name = 'risorseimmobiliari_it'
    external_source = "Risorseimmobiliari_PySpider_italy"

    # 1. FOLLOWING
    def start_requests(self):
        start_url = "https://www.risorseimmobiliari.it/case-in-affitto"
        yield Request(start_url,callback=self.parse,)
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'elenco_tipologie flexnav')]//a/@href[.!='#']").extract():
            if "appartament" in item or "loft" in item:
                prop_type = "apartment"
            elif "case" in item or "bungalow" in item or "ville" in item or "villini" in item or "terratetti" in item or "attici" in item or "bifamiliari" in item:
                prop_type = "house"
            elif "stanza" in item:
                prop_type = "room"
            else:
                continue
       
            follow_url = response.urljoin(item.split("#")[0])
            yield Request(follow_url, callback=self.jump_city, meta={"property_type": prop_type})
    
    def jump_city(self, response):
    
        for item in response.xpath("//div[contains(@class,'elenco_province ')]//li//a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            city = item.xpath(".//text()").get()
            yield Request(follow_url, callback=self.parse_listing, meta={"property_type": response.meta.get('property_type'),"city":city})

    def parse_listing(self, response):
        
        for item in response.xpath("//div[@id='section_list']//article//h2/a/@href").extract():
            if "//" in item:
                continue
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'),"city":response.meta.get('city')})
        
        next_page = response.xpath("//li[@class='page-item']/a[@title='Vai alla pagina successiva']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse_listing, meta={"property_type": response.meta.get('property_type'),"city":response.meta.get('city')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("city", response.meta.get('city'))
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//div[@class='heading-block noborder']/h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        external_id = response.xpath("//div[@class='detail']/div/label[contains(.,'Riferimento')]/following-sibling::span//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(' ')[-1].strip())
        
        rent = response.xpath("//div[@class='detail']/div/label[contains(.,'Prezzo')]/following-sibling::span//text()").get()
        if rent and "€" in rent:
            item_loader.add_value("rent", rent.replace(".","").split('€')[-1].replace(" ",""))
        item_loader.add_value("currency", "EUR")
        square_meters = response.xpath("(//div[@class='detail']/div/label[contains(.,'Superficie')]/following-sibling::span//text())[1]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].strip())
        room_count = response.xpath("//div[@class='camere']/span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('camere')[0].strip())
        bathroom_count = response.xpath("//div[@class='bagni']/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split('bagno')[0].strip())
        address = response.xpath("//div[@class='detail']/div/label[contains(.,'Indirizzo')]/following-sibling::span//text()").get()
        if address:
            item_loader.add_value("address", address)
        
        furnished = response.xpath("//div[@class='detail']/div/label[contains(.,'Arredato')]/following-sibling::span//text()").get()
        if furnished and "si" in furnished.lower():
            item_loader.add_value("furnished", True)
        elevator = response.xpath("//div[@class='detail']/div/label[contains(.,'Ascensore')]/following-sibling::span//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        balcony = response.xpath("//div[@class='detail']/div/label[contains(.,'Balcone')]/following-sibling::span//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        energy_label = response.xpath("//div[@class='progress-value']/div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        
        images = [x for x in response.xpath("//img[@class='sp-image']/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        description = "".join(response.xpath("//div[@id='section_descrizione']/p//text()").getall())
        if description:
            item_loader.add_value("description", description)
        latitude = response.xpath("//div[@id='section_mappa']/div[@class='hide']/span[@itemprop='latitude']//text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div[@id='section_mappa']/div[@class='hide']/span[@itemprop='longitude']//text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        landlord_name = response.xpath("//div[@class='agency-detail']/h3/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        yield item_loader.load_item()