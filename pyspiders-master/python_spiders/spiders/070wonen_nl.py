# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser


class MySpider(Spider):
    name = '070wonen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://070wonen.nl/huurwoningen/",
                "property_type" : "house"
            },
        ]# LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//ul[@id='search-results']/li//div[@class='search-result-title']/a/@href").extract():
            yield Request(item, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//span[.='volgende']/parent::a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "070wonen_PySpider_" + self.country + "_" + self.locale)
        
        title = response.xpath("//div[contains(@class,'card-unit-description')]//h3/following-sibling::p[1]/text()").extract_first()
        item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("?p=")[-1])

        price = response.xpath("//span[@class='page-price']/text()").extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[1])
        item_loader.add_value("currency", "EUR")

        item_loader.add_value("property_type", response.meta.get('property_type'))

        square = response.xpath("//li[span[. ='Oppervlakte']]/span[@class='spec-value']/text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m")[0])
        else:
            square = response.xpath("//div[@class='card card-unit-description']/div/p//text()[contains(.,'m2')]").get()
            if square:
                square = square.split("m2")[0].strip().split(" ")[-1].strip()
                item_loader.add_value("square_meters", square)

        images = [response.urljoin(x)for x in response.xpath("//div[@class='carousel-inner']/div/a/@href").extract()]
        if images:
                item_loader.add_value("images", images)

        item_loader.add_xpath("room_count","//li[span[. ='Slaapkamers']]/span[@class='spec-value']/text()")

        available_date = response.xpath("//li[span[. ='Beschikbaar']]/span[@class='spec-value']/text()[. !='Per direct']").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        desc = "".join(response.xpath("//div[@class='card card-unit-description']/div/p//text()").extract())
        desc = desc.replace("\n","").replace("\r","").replace("\t","").replace("\xa0","")
        if desc:
            item_loader.add_value("description", desc)

        if "huisdieren niet toegestaan" in desc.lower():
            item_loader.add_value("pets_allowed", False)
        
        if "Energieprestatiecertificaat:" in desc:
            energy_label = desc.split("Energieprestatiecertificaat:")[1].split("label")[1].split(".")[0].strip()
            if energy_label:
                item_loader.add_value("energy_label", energy_label)
        
        terrace = "".join(response.xpath("//dl[@class='unit-specs-list']/dt[.='Balkon']/following-sibling::dd[1]/i[@class='fa fa-fw fa-check text-success']").extract()).strip()
        if terrace:
            item_loader.add_value("balcony", True)

        terrace = "".join(response.xpath("//li[span[. ='Gestoffeerd']]/span[@class='spec-value']/text()").extract()).strip()
        if terrace:
            if "Ja" in terrace:
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False) 
            
        bathroom_count = response.xpath("//dt[.='Badkamers']/following-sibling::dd[1]/text()").get()
        if bathroom_count and bathroom_count.isdigit():
            item_loader.add_value("bathroom_count", bathroom_count.strip()) 

        address = "".join(response.xpath("normalize-space(//div[@class='col-12 pt-5']/span/text())").extract())

        if address:

            item_loader.add_value("zipcode", address.split(" ")[0])
            item_loader.add_value("address",address)
            item_loader.add_xpath("city", "normalize-space(//div[@class='col-12 pt-5']/h1/text())")

        else:
            item_loader.add_value("address",title.split(",")[1].split(",")[0])

        latlng = response.xpath("//div[@data-cord]/@data-cord").extract_first()
        if latlng:
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())

        item_loader.add_xpath("landlord_phone", "//span[@class='h3 d-block mb-2']/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='siteorigin-widget-tinymce textwidget']/p/span/a/text()")
        item_loader.add_value("landlord_name", "070Wonen")

        yield item_loader.load_item()