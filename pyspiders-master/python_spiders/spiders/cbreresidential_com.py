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
    name = 'cbreresidential_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    external_source = "Cbreresidential_PySpider_united_kingdom"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.cbreresidential.com/api/propertylistings/query?Site=uk-resi&CurrencyCode=GBP&RadiusType=Miles&Dynamic.UnderOffer=true%2Cfalse&Interval=Weekly&Common.HomeSite=uk-resi&Lat=51.5072178&Lon=-0.1275862&PolygonFilters=%5B%5B%2251.6723432%2C0.148271%22%2C%2251.38494009999999%2C0.148271%22%2C%2251.38494009999999%2C-0.3514683%22%2C%2251.6723432%2C-0.3514683%22%5D%5D&Common.Aspects=isLetting&PageSize=500&Page=1&_select=Dynamic.PrimaryImage,Common.ActualAddress,Common.Charges,Common.NumberOfBedrooms,Common.PrimaryKey,Common.UsageType,Common.Coordinate,Common.Aspects,Common.ListingCount,Common.IsParent,Common.HomeSite,Common.ContactGroup,Common.Highlights,Common.Walkthrough,Common.MinimumSize,Common.MaximumSize,Common.TotalSize,Common.GeoLocation,Common.Sizes",
                ],
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse
                )

    # 1. FOLLOWING
    def parse(self, response):
        data=json.loads(response.body)
        datar=data['Documents'][0]
        for i in datar:
            key=i['Common.PrimaryKey']
            try:
                adres2=i['Common.ActualAddress']['Common.PostalAddresses'][0]['Common.Line2'].lower().replace(", ","-").replace(" ","-")
            except:
                pass
            adres1=i['Common.ActualAddress']['Common.PostalAddresses'][0]['Common.Line1'].lower().replace(", ","-").replace(" ","-")
            url=f"https://www.cbreresidential.com/uk/en-GB/property/details/{key}/{adres1}-{adres2}/?view=isLetting"
            yield Request(url, callback=self.populate_item,meta={"i":i,"key":key})

            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item=response.meta.get("i")
        key=response.meta.get('key')
      
        latitude=item['Common.Coordinate']['lat']
        if latitude:
            item_loader.add_value("latitude",str(latitude))
        longitude=item['Common.Coordinate']['lon']
        if longitude:
            item_loader.add_value("longitude",str(longitude))
        try:
            adres=item['Common.ActualAddress']['Common.PostalAddresses'][0]['Common.Line2']+" "+item['Common.ActualAddress']['Common.PostalAddresses'][0]['Common.Line1']
            if adres:
                item_loader.add_value("address",adres)
        except:
            pass
        title=response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title",title)
        zipcode=item['Common.ActualAddress']['Common.PostCode']
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        rent=item['Common.Charges'][0]['Common.Amount']
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","GBP")
        features=str(item['Common.Aspects'])
        if features:
            if "isFurnished" in features:
                item_loader.add_value("furnished",True)
            if "hasBalcony" in features:
                item_loader.add_value("balcony",True)
            if "hasParking" in features:
                item_loader.add_value("parking",True)
            if "hasLift" in features:
                item_loader.add_value("elevator",True)
            if "hasGarden" in features:
                item_loader.add_value("terrace",True)
        room_count=item['Common.NumberOfBedrooms']
        if room_count:
            item_loader.add_value("room_count",room_count)

        url=f"https://www.cbreresidential.com/api/propertylisting/{key}?CurrencyCode=GBP&Interval=Weekly&Site=uk-resi"
        if url:
            yield Request(url, callback=self.otheritems,meta={"item_loader":item_loader})

    def otheritems(self, response):
        data=json.loads(response.body)
        item_loader=response.meta.get("item_loader")
        img=[]
        images=str(data['Document']['Common.Photos'])
        countdata=images.split("Common.ImageCaption")
        count=len(countdata)
        for i in range(0,int(count)-1):
            imag=data['Document']['Common.Photos'][i]['Common.ImageResources'][0]['Source.Uri']
            img.append(imag)
        item_loader.add_value("images",img)
        description=data['Document']['Common.LongDescription'][0]['Common.Text']
        if description:
            item_loader.add_value("description",description)
        property_type=data['Document']['Common.PropertyTypes'][0]
        if property_type:
            item_loader.add_value("property_type",property_type)
        available_date=data['Document']['Common.Availability']['Common.AvailabilityDate']
        if available_date:
            item_loader.add_value("available_date",available_date)




            

        yield item_loader.load_item()