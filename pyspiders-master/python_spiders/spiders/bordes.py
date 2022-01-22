# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import format_date
import json

class BordesSpider(scrapy.Spider):
    """ website_name """
    name = "bordes"
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    external_source = "Bordes_PySpider_belgium_nl"
    custom_settings={
        "HTTPCACHE_ENABLED":False
    }

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.bordes.be/api/properties.json?pg=1&order=postdate_desc&state=15&projects=false&zip=&near=&radius=10&exclude=&soldBy=&type=2865', 'property_type': 'apartment'},
            {'url': 'https://www.bordes.be/api/properties.json?pg=1&order=postdate_desc&state=15&projects=false&zip=&near=&radius=10&exclude=&soldBy=&type=3408', 'property_type': 'house'}
        ]
        for url in start_urls:
            print("request gitttttiiiiiiiiiii")
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response):

        all_data = json.loads((response.body).decode())


        yield scrapy.Request(
                url=response.url,
                callback=self.get_details,
                dont_filter=True,
                meta={'property_type': response.meta.get('property_type')}
            )

        if all_data["meta"]["pagination"]["links"].get("next"):
                yield scrapy.Request(
                url=all_data["meta"]["pagination"]["links"].get("next"),
                callback=self.parse,
                dont_filter=True,
                meta={'property_type': response.meta.get('property_type')}
            )


    def get_details(self, response):
        print("get_data,lsssc come")
        
        all_data = json.loads((response.body).decode())["data"]
        property_type = response.meta.get("property_type")

        for data in all_data:
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_source', self.external_source)
            item_loader.add_value("external_id",str(data["id"]))
            item_loader.add_value("title",data["title"])
            url = data["url"]
            item_loader.add_value("external_link",url)
            item_loader.add_value("rent",data["features"]["price"]["value"])
            item_loader.add_value("currency","EUR")
            item_loader.add_value("city",data["address"]["city"])
            item_loader.add_value("address",data["address"]["street1"]+" - "+data["address"]["city"])
            item_loader.add_value("zipcode",data["address"]["zip"])
            item_loader.add_value("landlord_name", "Bordes Izegem")
            item_loader.add_value("landlord_email", "izegem@bordes.be")
            item_loader.add_value("landlord_phone", "051 67 67 57")    
            item_loader.add_value("property_type",property_type)        


            yield scrapy.Request(url=url,
                                 callback=self.go_to_page,
                                 meta={'item_loader': item_loader})  




    def go_to_page(self,response):
        item_loader = response.meta.get("item_loader")
        desc = response.xpath("//div[@class='s-text-markup']//text()[last()]").get()
        if desc:
            item_loader.add_value("description",desc)
        else:
            desc = response.xpath("//div[@class='s-text-markup']/text()").get()
            if desc:
                item_loader.add_value("description",desc)
            else:
                desc = response.xpath("//div[@class='s-text-markup']/div/p/span/text()").get()
                if desc:
                    item_loader.add_value("description",desc)

        
        room_count = response.xpath("//p[text()='Slaapkamers']/following-sibling::p/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)

        square_meters = response.xpath("//p[text()='Bewoonbare opp.']/following-sibling::p/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(".")[0].replace("m²","").strip())

        images = response.xpath("//div[@id='lightbox']/a/@href").getall()
        if images:
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count",len(images))

        
        
        yield item_loader.load_item()











        # title = ' '.join(response.xpath("//div[@class='property-info']/div/text()").extract())
        # if title:
        #     item_loader.add_value("title", title.strip())
        # item_loader.add_value('external_link', response.url)
        
        # item_loader.add_value('property_type', response.meta.get('property_type'))
        # rent_string = response.css("div.property-info > div:nth-child(3) *::text").extract_first()
        # if rent_string:
        #     item_loader.add_value("rent_string", rent_string.strip())
        # item_loader.add_css('external_id', "p:contains('Referentiecode')+p *::text")
        # item_loader.add_css('square_meters', "p:contains('Bewoonbare opp')+p ::text")
        # item_loader.add_css('energy_label', "p:contains('EPC-waarde')+p ::text")
        # item_loader.add_css('floor', "p:contains('Verdieping')+p ::text")
        # available_date = ''.join(response.css("p:contains('Beschikbaar')+p ::text").extract())
        # if available_date:
        #     item_loader.add_value("available_date", format_date(available_date, "%d-%m-%Y"))
        # item_loader.add_css("description", "div.property-info + p *::text")
        # item_loader.add_css("images", "div#block-1 > a ::attr(href)")
        # item_loader.add_css("latitude", "div.lat::text")
        # item_loader.add_css("longitude", "div.lng::text")
        # item_loader.add_value("landlord_name", "Bordes")
        # item_loader.add_value("landlord_email", "info@bordes.be")
        # item_loader.add_value("landlord_phone", "+3251676757")
        # address = ''.join(response.css("div.property-info > div:nth-child(2) *::text").extract())
        # if address:
        #     item_loader.add_value('address', address)
        #     item_loader.add_value('city', address.split(" - ")[-1].strip())

        # utilities = ''.join(response.xpath("//p[contains(.,'kosten per maand')]/text()").extract())
        # if utilities:
        #     item_loader.add_value("utilities", utilities.split("kosten per maand")[-1].split("|")[0])
        # status = response.xpath("//div[@class='property-info']//strong[contains(.,'Verhuurd')]/text()").get()
        # if not status and (address and rent_string):
        
        

