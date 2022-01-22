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
    name = 'out_amsterdam_nl'
    execution_type = 'testing'
    country = 'netherlands' 
    locale = 'nl'
    external_source="Out_Amsterdam_PySpider_netherlands"

    def start_requests(self):
        yield Request("https://out-amsterdam.nl/actie/verhuur/", callback=self.parse)
    
    def parse(self, response):

        for item in response.xpath("//div[@class='property_listing_details']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": "apartment"})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")

        rent = "".join(response.xpath("//span[@class='price_area']/text()").getall())
        if rent:
            price = rent.replace(",", ".").replace(".", "")
            item_loader.add_value("rent_string",price)

        description = "".join(response.xpath("//div[@class='wpestate_property_description']/p/text()").getall())
        if rent:
            item_loader.add_value("description",description.strip())

        address = "".join(response.xpath("//span[@class='adres_area']//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        zipcode = "".join(response.xpath("//div[@class='listing_detail col-md-4']/strong[.='Postcode:']/following-sibling::text()").getall())
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())

        city = "".join(response.xpath("//div[@class='listing_detail col-md-4']/strong[.='Plaats:']/following-sibling::a/text()").getall())
        if city:
            item_loader.add_value("city",city.strip())
        addresscheck=item_loader.get_output_value("address")
        if not addresscheck:
            adres= "".join(response.xpath("//div[@class='wpestate_property_description']/p/text()").getall())
            if adres and "Amsterdam" in adres:
                city="Amsterdam"
                item_loader.add_value("city","Amsterdam")
            addres=response.xpath("//div[@class='listing_detail col-md-4']/strong[.='Land:']/following-sibling::text()").get()
            if addres:
                item_loader.add_value("address",addres+" "+city+" "+zipcode)


        external_id = "".join(response.xpath("//div[@class='listing_detail col-md-4']/strong[.='Object ID :']/following-sibling::text()").getall())
        if external_id:
            item_loader.add_value("external_id",external_id.strip())

        meters = "".join(response.xpath("//div[@class='listing_detail col-md-4']/strong[.='Woonoppervlak:']/following-sibling::text()").getall())
        if meters:
            item_loader.add_value("square_meters",meters.split("m")[0].strip())

        room_count = "".join(response.xpath("//div[@class='listing_detail col-md-4']/strong[.='Slaapkamers:']/following-sibling::text()").getall())
        if room_count:
            item_loader.add_value("room_count",room_count.strip())

        energy_label = "".join(response.xpath("//div[@class='listing_detail col-md-4']/strong[.='Energie klasse:']/following-sibling::text()").getall())
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())

        bathroom_count = "".join(response.xpath("//div[@class='listing_detail col-md-4']/strong[.='Badkamers:']/following-sibling::text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        LatLng = "".join(response.xpath("//script[@id='googlecode_property-js-extra']/text()[contains(.,'latitude')]").getall())
        if LatLng:
            lat = "".join(response.xpath("substring-before(substring-after(//script[@id='googlecode_property-js-extra']/text()[contains(.,'latitude')],'general_latitude'),',')").getall())
            lng = "".join(response.xpath("substring-before(substring-after(//script[@id='googlecode_property-js-extra']/text()[contains(.,'latitude')],'general_longitude'),',')").getall())
            item_loader.add_value("latitude",lat.replace('":"','').replace('"','').strip())
            item_loader.add_value("longitude",lng.replace('":"','').replace('"','').strip())

        images = [x.split("(")[1].split(")")[0].strip() for x in response.xpath("//div[contains(@class,'multi_image_slider_image')]/@style").extract()]
        if images is not None:
            item_loader.add_value("images", images)  

        furnished = "".join(response.xpath("//div[@class='listing_detail col-md-4']/strong[.='Opleveringsniveau:']/following-sibling::text()").getall())
        if furnished:
            item_loader.add_value("furnished", True)

        phone = response.xpath("normalize-space(//div[contains(@class,'agent_mobile_class')]/a/text())").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())
        else:
            item_loader.add_value("landlord_phone", "+31 (0)20 811 88 50")

        email = response.xpath("normalize-space(//div[contains(@class,'agent_email_class')]/a/text())").get()
        if email:
            item_loader.add_value("landlord_email", email)
        else:
            item_loader.add_value("landlord_email", "info@out-amsterdam.nl")

        name = response.xpath("normalize-space(//div[contains(@class,'agent_details')]/h3/a/text())").get()
        if name:
            item_loader.add_value("landlord_name", name.strip())
        else:       
            item_loader.add_value("landlord_name", "Out Amsterdam")

        
        yield item_loader.load_item()
