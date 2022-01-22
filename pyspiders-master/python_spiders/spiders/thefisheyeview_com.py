# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class TheFisheyeViewSpider(Spider):
    name = 'thefisheyeview_com'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.thefisheyeview.com"]
    start_urls = ["https://www.thefisheyeview.com/locazione-immobili/"]

    count = 0

    def parse(self, response):
        for url in response.css("ul.pagination li a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_page)

    def populate_page(self, response):
        for page in response.css("div.property_listing h4 a::attr(href)").getall(): 
            yield Request(response.urljoin(page), callback=self.populate_item)
    
    def populate_item(self, response):        
        data = {}
        title = response.css("h1.entry-title::text").get()
        if (("commerciale" in title) or ("ufficio" in title) or ("Magazzino" in title)):
            return

        property_type = "Apartment"

        external_id = response.css("#wpestate_property_description_section > strong:nth-child(6) > span:nth-child(1)::Text").get()

        rent = response.css(".price_area::text").get()

        description = response.css("#wpestate_property_description_section > p:nth-child(24)::text").get()

        area = response.css('span.adres_area a::text').getall()
        
        city = area[0]
        
        last_part_of_area = response.css('span.adres_area::text').getall()
        last_part_of_area = last_part_of_area[2].split(",")[1].strip()
        area.append(last_part_of_area)
        address = " ".join(area)

        
        images = response.css("div.multi_image_slider_image::attr(style)").getall()
        images_to_add = []
        for image in images:
            image_url = image.split("background-image:url(")[1]
            image_url = image_url.split(")")[0]
            images_to_add.append(image_url)
        
        bathroom_count = response.css("#wpestate_property_description_section > div:nth-child(3) > div:nth-child(2) > span:nth-child(3)::text").get()
        bathroom_count = bathroom_count.split(" ")

        room_count = response.css("#wpestate_property_description_section > div:nth-child(3) > div:nth-child(1) > span:nth-child(3)::text").get()
        room_count = room_count.split(" ")
        
        square_meters = response.css("#wpestate_property_description_section > strong:nth-child(6) > span:nth-child(1)::text").get()
        if(not square_meters):
            square_meters = response.css("#wpestate_property_description_section > p:nth-child(6) > strong:nth-child(1) > span:nth-child(1)::text").get()

        data["title"] = title 
        data["property_type"] = property_type
        data["external_id"] = external_id    
        data["rent"] = rent 
        data["description"] = description
        data["city"] = city
        data["address"] = address
        data["images"] = images_to_add
        data["bathroom_count"] = bathroom_count
        data["room_count"] = room_count
        data["square_meters"] = square_meters
        data["external_link"] = response.url

        contact_page = response.css("div.su-button-center a.su-button::attr(href)").get()
        yield Request(response.urljoin(contact_page), callback=self.get_contacts, meta={"data": data}, dont_filter = True)
        

    def get_contacts(self, response):
        item_loader = ListingLoader(response=response)
        
        data = response.meta.get("data")
        
        landlord_name = "FISHEYE VIEW"
        landlord_phone = response.css(".vc_col-sm-8 > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > div:nth-child(1) > p:nth-child(1) > span:nth-child(3)::text").get()
        landlord_email = response.css(".vc_col-sm-8 > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > div:nth-child(1) > p:nth-child(1) > span:nth-child(5) > a:nth-child(1)::attr(title)").get()

        item_loader.add_value("external_link", data["external_link"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", data["external_id"])
        item_loader.add_value("property_type", data["property_type"])
        item_loader.add_value("address", data["address"])
        item_loader.add_value("rent_string", data["rent"])
        item_loader.add_value("title", data["title"])
        item_loader.add_value("description", data["description"])
        item_loader.add_value("city", data["city"])
        item_loader.add_value("square_meters", data["square_meters"])
        item_loader.add_value("images", data["images"])
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        try:
            item_loader.add_value("room_count", int(data["room_count"][0]))
        except ValueError:
            pass
                
        try:
            item_loader.add_value("bathroom_count", int(data["bathroom_count"][0]))
        except ValueError:
            pass

        yield item_loader.load_item()

