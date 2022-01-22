import scrapy
import re
from python_spiders.loaders import ListingLoader


class MydreamrealtyvancouverSpider(scrapy.Spider):
    name = 'mydreamrealtyvancouver'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['mydreamrealtyvancouver.com']
    
    
    def start_requests(self):
        start_urls = ['https://www.mydreamrealtyvancouver.com/unfurnished.html/',
                      'https://www.mydreamrealtyvancouver.com/furnished-rental.html/']
        
        for list_pages in start_urls:
            pages = [list_pages+str(i) for i in range(0, 100, 10)]
            for page in pages:
                yield scrapy.Request(url=page, callback=self.parse)

    def parse(self, response):
       
        for item in response.css("div.img a::attr(href)").getall():
            yield scrapy.Request(url=item, callback=self.parse_page)
        
        

        
    
    
    def parse_page(self, response):
        
        images                      = response.css("div.feature-area.add .slideshow div div::attr(style)").re(r"https://.*.jpg")
        external_id                 = response.css("div.txt-holder.txt-holder-1920 .txt.add::text").get().strip()
        address                     = response.css("div.location div p[itemprop=streetAddress]::text").get()
        city                        = response.css("div.location div p[itemprop=addressLocality]::text").get()+","+response.css("div.location div p[itemprop=addressRegion]::text").get()
        zipcode                     = response.css("div.location div p[itemprop=postalCode]::text").get()
        
        rent                        = make_rent(response.css("div.txt-holder .txt.add::text").getall()[1].strip())
        square_meters               = make_square(response.css("div.txt-holder .txt.add span::text").getall()[0].strip())
        
        description, pets_allowed, property_type   = make_desc("".join(response.css("div.container .caption .box1 div::text").getall()).strip())
        
        if description == '':
            description,pets_allowed, property_type    = make_desc("--".join(response.css(".box1 ::text").getall()))
        else:
            description, pets_allowed, property_type   = make_desc("--".join(response.css("div.container .caption .box1 div::text").getall()).strip())
        
        
        landlord_phone, landlord_email      = response.css("ul.links li a::text").getall()
        room_count, bathroom_count, parking = get_vals(response.css("ul.area.add.icon li span::text").getall())
        
        
        
        
        item = ListingLoader(response=response)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("external_link"          ,response.url)
        item.add_value("external_id"            ,external_id)
        item.add_value("title"                  ,property_type + " in "+address)
        item.add_value("property_type"          ,property_type)
        item.add_value("city"                   ,city)
        item.add_value("zipcode"                ,zipcode)
        item.add_value("address"                ,address)
        item.add_value("rent"                   ,rent)
        item.add_value("images"                 ,images)
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("square_meters"          ,int(int(square_meters)*10.764))
        item.add_value("parking"                ,parking == "yes")
        item.add_value("description"            ,description)
        item.add_value("currency"               ,"USD")
        item.add_value("landlord_email"         ,landlord_email)
        item.add_value("landlord_phone"         ,landlord_phone)
        
        item.add_value("pets_allowed"           ,pets_allowed)
        
        if rent not in ["Rented", "/month"]:
            yield item.load_item()

def make_desc(desc):
    types = ['apartment', "penthouse", 'house', 'studio']
    pets_allowed = ""
    property_type = 'apartment'
    desc =desc.lower()
    i = desc.find("* this")
    desc = removeLines(desc)
    if i > -1:
        desc = desc[:i].replace("* this","").replace("--","\n")
    else:
        if desc.find("*this") > -1:
             desc = desc[:desc.find("*this")].replace("*this","").replace("--","\n")
        else:
            desc = desc[:desc.find("managed by")].replace("managed by","").replace("--","\n")
    
    desc = desc.replace("consulting services and is marketed/listed by my dream","").replace("this property is","").replace("liste","")
    desc = re.sub("unit is manage.*","",desc)
    desc = re.sub("property is manage.*","",desc)
    
     
    for t in types:
        if t in desc and t in ["penthouse", "condo"]:
            property_type = "apartment"
            break
        if t in desc:
            property_type = t
            break
            
    if "No pets" in desc:
        pets_allowed = False

        
    return desc , pets_allowed, property_type



def removeLines(desc):
    des = ''
    for w in desc:
        if w in ['\r','\n']:
            continue
        else:
            des += w
    return des
def make_square(square):
    return round(eval(re.findall("[0-9]+",square)[0]+"*0.0929"))

def make_rent(rent):
    if rent in ["Rented", "/month"]:
        return rent
    return int(re.findall("[0-9]+,*[0-9]*", rent)[0].replace(",",""))

def get_vals(items):
    if items[0] == "Studio":
        return 1, int(float(items[1])), int(float(items[2])) > 0
    return int(float(items[0])), int(float(items[1])), int(float(items[2])) > 0
