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
    name = 'wilsons_pl'
    execution_type = 'testing'
    country = 'Poland'
    locale = 'pl'
    external_source = "Wilsons_PySpider_Poland_pl"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://wilsons.pl/znajdz-nieruchomosc/?_typ=mieszkanie&_rodzaj=wynajem",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://wilsons.pl/znajdz-nieruchomosc/?_typ=dom&_rodzaj=wynajem"
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//h6[@class='elementor-heading-title elementor-size-default']//a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = f"https://wilsons.pl/znajdz-nieruchomosc/?_typ=mieszkanie&_rodzaj=wynajem&_paged={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("(//title//text())[1]").get()
        if title:
            title = title.replace("\u017c","").replace("\u015b","").replace("\u0119","").replace("\u0142","").replace("\u00f3","").replace("\u0141","").replace("\u0144","").replace("\u0105","").replace("\u0118","").replace("\u017b","")
            item_loader.add_value("title",title)

        external_id=response.xpath("//p[contains(.,'Numer oferty')]//b//text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)

        address=response.xpath("(//div[@class='elementor-text-editor elementor-clearfix']//text())[1]").get()
        if address:
            item_loader.add_value("address",address)

        city = "".join(response.xpath("(//div[@class='elementor-text-editor elementor-clearfix']//text())[1]").get())
        if city:
            if city and "," in city:
                city = city.split(",")[-1]
            item_loader.add_value("city",city)    

        description=response.xpath("//div[@class='elementor-text-editor elementor-clearfix']//text()").getall()
        if description:
            item_loader.add_value("description",description)

        rent=response.xpath("(//h1[@class='elementor-heading-title elementor-size-default']//text())[2]").get()
        if rent:
            rent = rent.replace(" ","")
            if rent and "PLN" in rent.lower():
                rent = rent.split("PLN")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","PLN")

        room_count=response.xpath("//p[contains(.,'Liczba pomieszczeń')]//parent::div//h4//span//text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
            
        bathroom_count=response.xpath("//p[contains(.,'Liczba łazienek:')]//b//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        floor=response.xpath("//p[contains(.,'Piętro:')]//b//text()").get()
        if floor and "0" not in floor:
            item_loader.add_value("floor",floor)

        available_date=response.xpath("//p[contains(.,'Data aktualizacji:')]//b//text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)

        square_meters=response.xpath("//p[contains(.,'Powierzchnia ')]//parent::div//h4//span//text()[contains(.,'m')]").get()
        if square_meters:
            if square_meters and "." in square_meters:
                square_meters=square_meters.split(".")[0]
            elif square_meters and "m" in square_meters.lower():
                square_meters=square_meters.split("m")[0]
            item_loader.add_value("square_meters",square_meters)

        images = [response.urljoin(x) for x in response.xpath("//div[@class='gallery-main']//a[@class='slide']//@href").extract()]
        if images:
            item_loader.add_value("images", images)

        landlord_name=response.xpath("(//h4[@class='elementor-image-box-title']//text())[1]").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)

        landlord_phone=response.xpath("//a[@id='telefon_caly_2']//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone",landlord_phone)
            
        landlord_email=response.xpath("//div[@class='gen-email']//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email",landlord_email)

        yield item_loader.load_item()