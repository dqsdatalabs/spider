# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
 
import scrapy, copy, urllib
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces
import re
from scrapy import Request
import dateparser
class VastengoedData(scrapy.Spider):
    
    name = 'vastengoed_be'
    allowed_domains = ['vastengoed.be']
    start_urls = ['https://www.vastengoed.be']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'nl'
    thousand_separator = '.'
    scale_separator = ','
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.vastengoed.be/nl-be/te-huur?pageindex=1&Categories=&EstateTypes=&ExcludeProjectRelatedItems=True&FlowStatus=&marketingtypes-excl=&MinimumRentedPeriod=&MinimumSoldPeriod=&OpenHouse=&price-to=&reference-notlike=&RentedPeriod=0&SoldPeriod=0&sorts=Flat&sorts[]=Flat&transactiontype=Rent",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.vastengoed.be/nl-be/te-huur?MinimumSoldPeriod=&SoldPeriod=0&MinimumRentedPeriod=&RentedPeriod=0&FlowStatus=&ExcludeProjectRelatedItems=True&EstateTypes=&OpenHouse=&Categories=&marketingtypes-excl=&reference-notlike=&sorts=Dwelling&transactiontype=Rent&sorts%5B%5D=Dwelling&price-to=",
                ],
                "property_type" : "house"
            }
            
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//article//div[@class='card-text']//a/@href").getall():
            yield Request(response.urljoin(item), callback=self.get_property_details, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//div[@class='offer-head__item pagination']//a[strong[@class='pagination-arrow next']]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url.split("?")[0])
        item_loader.add_xpath('external_id', '//tr[td[.="Referentie:"]]/td[2]/text()')
        item_loader.add_xpath('title', './/h1/span/text()')
        item_loader.add_value('property_type', response.meta["property_type"])
        item_loader.add_value("external_source", "Vastengoed_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_xpath('description', '//div[@class="detail--text__container"]//text()[normalize-space()]')

        item_loader.add_xpath('address', './/tr[td[.="Adres:"]]/td[2]/text()') 
        zipcode=response.xpath("//title/text()").get()
        if zipcode:
            zipcode=re.findall("\d+",zipcode)
            item_loader.add_value("zipcode",zipcode[-1])
        item_loader.add_xpath('city', './/tr[td[.="Adres:"]]/td[2]/text()[last()]')

        item_loader.add_xpath('rent_string', '//tr[td[.="Prijs:"]]/td[2]/text()')
        item_loader.add_xpath('room_count', '//tr[td[.="Slaapkamers:"]]/td[2]/text()')
        room_count=response.xpath("//div[@class='detail--text__container']/p/text()").getall()
        if room_count:
            for i in room_count:
                if "slaapkamer" in i.lower():
                    room_count=re.findall("\d+",i)
                    if room_count:
                        item_loader.add_value("room_count",room_count)
        item_loader.add_xpath('bathroom_count', '//tr[td[.="Badkamers:"]]/td[2]/text()')
        item_loader.add_xpath('energy_label', 'substring-before(//tr[td[.="EPC Index:"]]/td[2]/text(),",")')
        available_date = response.xpath('//tr[td[.="Beschikbaar vanaf:"]]/td[2]/text()').extract_first()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)
        parking = response.xpath('//tr[td[.="Parking:" or .="Garage:"]]/td[2]/text()').extract_first()
        if parking:
            if parking == "0": item_loader.add_value("parking", False)
            else: item_loader.add_value("parking", True)
        parkingcheck=item_loader.get_output_value("parking")
        if not parkingcheck:
            parking=response.xpath("//div[@class='detail--text__container']/p//text()").getall()
            if parking:
                for i in parking:
                    if "autostaanplaats" in i.lower() or "garage" in i.lower():
                        item_loader.add_value("parking",True)


        terrace = response.xpath('//tr[td[.="Terras:"]]/td[2]/text()').extract_first()
        if terrace:
            if terrace != "Ja": item_loader.add_value("terrace", False)
            else: item_loader.add_value("terrace", True)

        elevator = response.xpath('//tr[td[.="Lift:"]]/td[2]/text()').extract_first()
        if elevator:
            if elevator != "Ja": item_loader.add_value("elevator", False)
            else: item_loader.add_value("elevator", True)
        latlng=response.xpath("//script[@type='application/ld+json']/text()").get()
        if latlng:
            latitude=latlng.split("'geo':{")[-1].split("latitude")[-1].split(",")[0].replace("\\","").split(":")[-1].replace('"',"")
            item_loader.add_value("latitude",latitude)
            longitude=latlng.split("'geo':{")[-1].split("longitude")[-1].split(",")[0].replace("\\","").split(":")[-1].replace('"',"")
            item_loader.add_value("longitude",longitude)
        item_loader.add_value('landlord_name', 'Vast & Goed Makelaars')
        item_loader.add_value('landlord_email', 'info@vastengoed.be')
        item_loader.add_value('landlord_phone', '014 72 73 74')
        images = [x for x in response.xpath("//div[@id='slideshow']//a[@class='detail-image']/@href").extract()]
        if images:
            item_loader.add_value("images", images) 
        yield item_loader.load_item()
