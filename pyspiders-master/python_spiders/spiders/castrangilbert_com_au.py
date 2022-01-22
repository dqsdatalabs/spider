# -*- coding: utf-8 -*-

from scrapy.loader.processors import MapCompose
from scrapy import Spider, item
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import math


class CastrangilbertComAuSpider(Spider):
    name = "castrangilbert_com_au"
    allowed_domains = ['cornelis-partners']
    start_urls = ['https://castrangilbert.com.au/rent/']
    execution_type = 'testing'
    external_source = "Castrangilbert_com_au_PySpider_australia"
    country = 'australia'
    locale ='au'


    form_data = {
        "action": "ajax_archive",
        "post_type": "property",
        "property_tag[]": "current",
        "listing_type[]": "rental",
        "listing_status[]": "current",
        "cpt_page_id": "13",
        "post_status[]": "publish",
        "meta[min:rent]": "",
        "meta[max:rent]": "",
        "meta[min:features_bedrooms]": "0",
        "meta[max:features_bedrooms]": "0",
        "meta[min:features_bathrooms]": "0",
        "meta[max:features_bathrooms]": "0",
        "meta[min:features_garages]": "0",
        "sortby": "date DESC",
        "archive_view": "list",
        "n_pages": "11",
        "paged": "1",
    }

    headers = {
        "authority": "castrangilbert.com.au",
        "method": "POST",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    def start_requests(self):

            total_page_num = int(self.form_data["n_pages"])
            page_num = 1
            while page_num <= total_page_num:
                with open("page_num.file","a",encoding='utf-8') as file:
                    file.write(str(page_num)+"\n")
                    
                self.form_data["paged"] = str(page_num)
                yield FormRequest(self.start_urls[0],
                            callback=self.parse,
                            headers=self.headers,
                            formdata=self.form_data,
                            dont_filter=True,
                            )
                page_num += 1


    def parse(self, response):
        for item in response.xpath("//article[contains(@class,'post')]"):
            follow_url = item.xpath(".//a/@href").get()
            yield Request(url=follow_url,
                        callback=self.get_property_details,
                        dont_filter=True
                        )
            

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link",response.url)
        item_loader.add_value("external_source",self.external_source)
        
        title = response.xpath("//h1[@class='detail-suburb']/text()").get()
        if title:
            item_loader.add_value("title",title)

        images_content = response.xpath("//div[contains(@class,'slide slider__item contain')]/img/@data-flickity-lazyload").getall()
        if images_content:
            images = []
            for image in images_content:
                img = "https://castrangilbert.com.au/" + image
                images.append(img)
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count",len(images))


        rent = response.xpath("//span[text()='Bond']/following-sibling::span/text()").get()
        if rent:
            rent = rent.split(".")[0].replace(",","")
            item_loader.add_value("rent",rent)

        landlord_name = response.xpath("//div[@class='property-agents__item-details']/h5/a/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name)

        # landlord_phone = response.xpath("//div[@class='property-agents__item-details-phone']/a/text()").get()
        # if landlord_phone:
        #     item_loader.add_value("landlord_phone",landlord_phone)

        # landlord_email = response.xpath("//div[@class='property-agents__item-details-email']/a/@href").get()
        # if landlord_email:
        #     landlord_email = landlord_email.split("to:")[-1]
        #     item_loader.add_value("landlord_email",landlord_email)

        desc = response.xpath("//span[@class='trim-html']/p").getall()
        if desc:
            description = " ".join(desc)
            item_loader.add_value("description",description)

        room = response.xpath("//span[@class='theme-icon theme-icon-bed']/following-sibling::span/text()").get()
        if room:
            item_loader.add_value("room_count",room)

        bathroom_count = response.xpath("//span[@class='theme-icon theme-icon-bath']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        parking = response.xpath("//span[@class='theme-icon theme-icon-car']/following-sibling::span/text()").get()
        if parking:
            item_loader.add_value("parking",True)

        street = response.xpath("//h2[@class='detail-street']/text()").get()
        if street:
            address = title + " " + street
            item_loader.add_value("address",address)

        zipcode = response.xpath("//span[@class='detail-pc-state']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
            
        item_loader.add_value("city","Victoria")
        item_loader.add_value("currency","AUD")

        if "house" in description:
            item_loader.add_value("property_type","house")
        else:
            item_loader.add_value("property_type","apartment")
        latitude=response.xpath("//section[@class='single-property__content page-section']//div/@data-lat").extract()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=response.xpath("//section[@class='single-property__content page-section']//div/@data-lng").extract()
        if longitude:
            item_loader.add_value("longitude",longitude)

        external_id = (response.url).strip("/").split("-")[-1]
        item_loader.add_value("external_id",external_id)
        item_loader.add_value("landlord_email","cgrental@castrangilbert.com.au")
        item_loader.add_value("landlord_phone","+61 3 9827 1177")

        yield item_loader.load_item()
        